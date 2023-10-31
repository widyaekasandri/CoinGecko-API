import json
import logging
import os
import sys
import time
import awswrangler as wr
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from io import BytesIO
from uuid import uuid4
import boto3
import requests
from urllib3.util import Retry

logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "[%(asctime)s] %(name)s - %(levelname)s | {%(filename)s:%(lineno)d} %(message)s"
)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)

logger.handlers.clear()
logger.addHandler(stream_handler)

retry = Retry(total=3, backoff_factor=1)
s = requests.Session()
s.mount("https://", requests.adapters.HTTPAdapter(max_retries=retry))

s3 = boto3.resource("s3")


def handler(event, context):

    print('Trigger function the first time')
    region_name = os.environ.get("REGION_NAME")
    bucket_name = os.environ.get("S3_BUCKET")
    coins_bucket = os.environ.get("COINS_BUCKET")
    coins_dir = os.environ.get("COINS_DIR")
    coins_key = os.environ.get("COINS_KEY")
    base_uri = os.environ.get("BASE_URI")

    session = boto3.Session(region_name=region_name)

    logger.info(
        f"Fetching currencies from s3://{coins_bucket}/{coins_dir}/{coins_key}")
    coins_data = s3.Object(bucket_name=coins_bucket, key=f"{coins_dir}/{coins_key}").get()
    coins_data = coins_data["Body"].read().decode("utf-8")
    coins_data = json.loads(coins_data)

    trigger_time = event.get("time")
    if trigger_time:
        trigger_time = datetime.strptime(trigger_time, "%Y-%m-%dT%H:%M:%SZ")
        today = trigger_time.replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
        )
    else:
        print("No time found in event")
        today = datetime.now()
        today = today.replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
        )



    # for hourly pull intraday

    tomorrow = today + timedelta(days=1)
    time_start = int(today.timestamp())
    time_end = int(tomorrow.timestamp())
    year = today.year
    month = today.month
    day = today.day

    output_data = BytesIO()

    bucket = s3.Bucket(bucket_name)

    params = {"vs_currency": "usd", "from": time_start, "to": time_end}
    logger.info(params)

    # Delete previous contents

    cg_object_prefix = f"/incoming/{year}/{month}/{day}/"
    cg_bucket = 's3://' + bucket_name + cg_object_prefix

    # Delete existing contents
    try:
        wr.s3.delete_objects(cg_bucket, boto3_session=session)
        logger.info(f"Previous contents in {cg_bucket} have been deleted")
    except:
        logger.info(f"No contents to delete from {cg_bucket}")

    # Pull rates and write contents

    count = 0
    for coin in coins_data:
        try:
            logger.info(f"Querying for {coin}")
            r = s.get(
                f"{base_uri}/{coin['id']}/market_chart/range", params=params)
            logger.info(r.status_code)
            data = r.json()
            prices = data["prices"]
            for record in prices:
                d = {
                    "currency": coin["id"],
                    "currency_code": coin["symbol"],
                    "last_updated_at": record[0],
                    "usd_rate": record[1],
                }

                d = json.dumps(d) + "\n"
                output_data.write(d.encode("utf-8"))

            count += 1
            unique_id = str(uuid4())
            key_path = (
                f"incoming/{year}/{month}/{day}/{unique_id}_{coin['symbol']}.json"
            )
            logger.info(f"Writing to {key_path}")
            output_data.seek(0)
            bucket.upload_fileobj(Fileobj=output_data, Key=key_path)

            output_data = BytesIO()
            time.sleep(1.2)
            if count % 20 == 0:
                logger.info(f"Dumped {count} currencies.")
        except KeyError as e:
            logger.error(f"Failed to get {coin}")
            logger.error(e)
            logger.error(r.text)
    logger.info(f"Completed dumping {count} currencies.")