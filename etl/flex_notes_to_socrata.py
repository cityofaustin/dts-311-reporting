"""
Downloads flex question/answers for CSRs data from a CSV report endpoint and then uploads the data to a Socrata dataset
"""

import os
import logging

import pandas as pd
from sodapy import Socrata

from field_maps import FLEX_NOTES_MAP
import utils

# Socrata Secrets
SO_WEB = os.getenv("SO_WEB")
SO_TOKEN = os.getenv("SO_TOKEN")
SO_KEY = os.getenv("SO_KEY")
SO_SECRET = os.getenv("SO_SECRET")
DATASET = os.getenv("FLEX_NOTES_DATASET")

# CSR CSV data endpoint
ENDPOINT = os.getenv("FLEX_NOTE_ENDPOINT")

# Questions we do not want to store
IGNORED_QUESTIONS = [
    "Mobile Apps Reporter Information",
    "Provide license plate number of vehicle, if known.",
    "*What are the last 4 digits of the card number that you used for this transaction?",
    "Enter APD  # if known (Case #, Officer #, etc.).",
    "Provide User ID from ParkATX App (If user ID is unavailable, type N/A)",
    "What are the last 4 digits of the card number that you used for this transaction?",
    "What is the 9-digit citation number (top line)?",
]


def transform(df):
    logger.info("Transforming Flex Notes")

    # Field mapping
    df = df[list(FLEX_NOTES_MAP.keys())]
    df.rename(columns=FLEX_NOTES_MAP, inplace=True)

    # Dropping unwanted questions from our dataset
    df = df[~df["flex_question"].isin(IGNORED_QUESTIONS)]

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

    logger.info("Uploading flex note records to Socrata")
    res = utils.load_to_socrata(client=soda, dataset_id=DATASET, data=data)
    logger.info(res)

    return res


if __name__ == "__main__":
    logger = utils.get_logger(
        __name__,
        level=logging.INFO,
    )
    main()
