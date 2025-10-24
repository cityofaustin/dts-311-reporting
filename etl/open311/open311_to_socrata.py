"""
Checks open311 API for new CSRs and updates a Socrata Dataset
"""

import argparse
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os
import logging
import requests
import time

from sodapy import Socrata

from etl import utils

# Socrata Secrets
SO_WEB = os.getenv("SO_WEB")
SO_TOKEN = os.getenv("SO_TOKEN")
SO_KEY = os.getenv("SO_KEY")
SO_SECRET = os.getenv("SO_SECRET")
REALTIME_DATASET = os.getenv("REALTIME_DATASET")

# Open 311 API Key
API_KEY = os.getenv("OPEN_311_API_KEY")
API_BASE_URL = os.getenv("OPEN_311_API_BASE_URL")

headers = {"Authorization": f"Bearer {API_KEY}"}

# 10 requests per minute API limit
MAX_REQUESTS_PER_MINUTE = 10
SECONDS_PER_REQUEST = 60 / MAX_REQUESTS_PER_MINUTE

# Timezone objects
CENTRAL_TZ = ZoneInfo("America/Chicago")
UTC_TZ = timezone.utc


def determine_query_time(client, override_date=None):
    """
    Uses provided date arg first, and subtracts 10 minutes to reduce the risk we miss any updated records
    If no arg is provided, the date used is the most recently updated socrata record.
    If the dataset is empty, default to Oct 13, 2025.
    """
    if override_date:
        try:
            # Parse ISO 8601 datetime and subtract 10 minutes
            dt = datetime.fromisoformat(override_date)
            adjusted = dt - timedelta(minutes=10)
            adjusted_str = adjusted.isoformat()
            adjusted_str = adjusted_str.replace("+00:00", "Z")
            logger.info(f"Using provided date (minus 10 minutes): {adjusted_str}")
            return adjusted_str
        except ValueError:
            raise ValueError(
                f"Invalid date format: {override_date}. Expected ISO 8601 format."
            )

    # get last updated record from socrata
    date = client.get(
        REALTIME_DATASET,
        select="updated_datetime",
        order="updated_datetime DESC",
        limit=1,
    )
    if date:
        # Interpret as Central Time and convert to UTC
        ct_time = datetime.fromisoformat(date[0]["updated_datetime"]).replace(
            tzinfo=CENTRAL_TZ
        )
        utc_time = ct_time.astimezone(UTC_TZ)
        adjusted_str = utc_time.isoformat()
        adjusted_str = adjusted_str.replace("+00:00", "Z")
        return adjusted_str
    return "2025-10-13T00:00:00Z"


def socrata_point_location_formatting(rec):
    """
    Formats latitude and longitude for "Point" Socrata datatype
    """
    if rec["long"] and rec["lat"]:
        return f"POINT ({rec['long']} {rec['lat']})"
    return None


def convert_to_central_and_strip_tz(rec, fields):
    """
    Converts UTC timestamps to Central Time and removes timezone info
    (since Socrata stores floating datetimes in local time).
    """
    for f in fields:
        if rec.get(f):
            dt = datetime.fromisoformat(rec[f].replace("Z", "+00:00")).astimezone(
                CENTRAL_TZ
            )
            rec[f] = dt.replace(tzinfo=None).isoformat()
    return rec


def main(args):
    # Logging into socrata
    soda = Socrata(
        SO_WEB,
        SO_TOKEN,
        username=SO_KEY,
        password=SO_SECRET,
        timeout=500,
    )

    # Determining what time bounds we should ask Open311 for.
    query_time = determine_query_time(soda, args.date)

    keep_going = True
    page = 1
    while keep_going:
        start = time.time()

        # Getting data from open311
        res = requests.get(
            API_BASE_URL
            + f"/requests.json?order=updated_datetime&updated_after={query_time}&per_page=100&page={page}",
            headers=headers,
        )
        res.raise_for_status()
        data = res.json()

        # If we get nothing back from Open311 we are done processing.
        if not data:
            keep_going = False
            continue

        # Processing data
        for record in data:
            record["location"] = socrata_point_location_formatting(record)
            record = convert_to_central_and_strip_tz(
                record, ["updated_datetime", "requested_datetime"]
            )

        # Send data to socrata
        res = utils.load_to_socrata(soda, REALTIME_DATASET, data, method="upsert")
        logger.info(res)

        # preventing timeouts from Open311
        elapsed = time.time() - start
        sleep_time = max(0, SECONDS_PER_REQUEST - elapsed + 0.1)
        time.sleep(sleep_time)

        # Go to next page of results.
        page += 1


if __name__ == "__main__":
    logger = utils.get_logger(
        __name__,
        level=logging.INFO,
    )
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Fetch and update Open311 data in Socrata"
    )
    parser.add_argument(
        "-d",
        "--date",
        help="Optional ISO 8601 date (e.g. 2025-10-15T06:55:01.132759+00:00) to start query from",
        required=False,
    )
    args = parser.parse_args()
    main(args)
