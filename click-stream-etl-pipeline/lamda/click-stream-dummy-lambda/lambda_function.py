import json
import uuid
import random
import boto3
from datetime import datetime, timezone, timedelta

# ---------- CONFIG ----------
KINESIS_STREAM_NAME = "kinesis-click-real-data"
TOTAL_EVENTS_PER_RUN = 100
USER_ID_START = 10001
USER_ID_END = 10100

COUNTRIES = ["IN", "CA", "JP", "AU", "NL"]

COUNTRY_DEVICE_MAP = {
    "IN": ["mobile", "mobile", "desktop"],
    "CA": ["desktop", "mobile", "tablet"],
    "JP": ["mobile", "desktop"],
    "AU": ["desktop", "mobile"],
    "NL": ["desktop", "tablet"]
}

PAGES = [
    "/home",
    "/search",
    "/product",
    "/cart",
    "/checkout",
    "/profile"
]
# ----------------------------

kinesis_client = boto3.client("kinesis")

def lambda_handler(event, context):
    run_id = str(uuid.uuid4())
    records = []

    for _ in range(TOTAL_EVENTS_PER_RUN):
        user_id = random.randint(USER_ID_START, USER_ID_END)
        session_id = str(uuid.uuid4())

        country = random.choice(COUNTRIES)
        device = random.choice(COUNTRY_DEVICE_MAP[country])

        click_time = datetime.now(timezone.utc)
        session_duration = random.randint(5, 180)
        leave_time = click_time + timedelta(seconds=session_duration)

        click_event = {
            "run_id": run_id,
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "session_id": session_id,
            "country": country,
            "device": device,
            "event_type": "click",
            "page": random.choice(PAGES),
            "click_time": click_time.isoformat(),
            "leave_time": leave_time.isoformat(),
            "session_duration_sec": session_duration
        }

        records.append({
            "Data": json.dumps(click_event),
            "PartitionKey": run_id   # 🔥 IMPORTANT CHANGE
        })

    response = kinesis_client.put_records(
        StreamName=KINESIS_STREAM_NAME,
        Records=records
    )

    return {
        "statusCode": 200,
        "run_id": run_id,
        "total_records": TOTAL_EVENTS_PER_RUN,
        "failed_records": response.get("FailedRecordCount", 0)
    }