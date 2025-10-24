"""
Downloads activities for CSRs data from a CSV report endpoint and then uploads the data to a Socrata dataset
"""

import os
import logging

import pandas as pd
from sodapy import Socrata

from etl.field_maps import ACTIVITIES_MAP
from etl import utils

# Socrata Secrets
SO_WEB = os.getenv("SO_WEB")
SO_TOKEN = os.getenv("SO_TOKEN")
SO_KEY = os.getenv("SO_KEY")
SO_SECRET = os.getenv("SO_SECRET")
DATASET = os.getenv("ACTIVITIES_DATASET")

# CSR CSV data endpoint
ENDPOINT = os.getenv("ACTIVITIES_ENDPOINT")


def transform(df):
    logger.info("Transforming Activities")

    # Field mapping
    df = df[list(ACTIVITIES_MAP.keys())]
    df.rename(columns=ACTIVITIES_MAP, inplace=True)

    # date column formatting to match format expected by Socrata
    df = utils.transform_datetime_formats(df)

    # Remove rows without an activity id, this happens when a CSR has no activities associated with it
    df = df.dropna(subset=["activity_id"])

    # Replacing missing values with None instead of the default NaN pandas uses
    df.replace({pd.NA: None}, inplace=True)

    payload = df.to_dict("records")
    return payload


def main():
    soda = Socrata(
        SO_WEB,
        SO_TOKEN,
        username=SO_KEY,
        password=SO_SECRET,
        timeout=500,
    )

    data = utils.extract(endpoint=ENDPOINT, logger=logger)
    data = transform(data)
    logger.info("Uploading activity records to Socrata")
    res = utils.load_to_socrata(
        client=soda, dataset_id=DATASET, data=data, method="upsert"
    )
    logger.info(res)

    return res


if __name__ == "__main__":
    logger = utils.get_logger(
        __name__,
        level=logging.INFO,
    )
    main()
