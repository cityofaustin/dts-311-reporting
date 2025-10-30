"""
Downloads 311 requests data from a CSV report endpoint and then uploads the data to a Socrata dataset
"""

import os
import logging

import pandas as pd
import numpy as np
from sodapy import Socrata
from pyproj import Transformer

from etl.field_maps import REQUESTS_MAP
from etl import utils

# Socrata Secrets
SO_WEB = os.getenv("SO_WEB")
SO_TOKEN = os.getenv("SO_TOKEN")
SO_KEY = os.getenv("SO_KEY")
SO_SECRET = os.getenv("SO_SECRET")
DATASET = os.getenv("REQUESTS_DATASET")

# Request CSV report endpoint
ENDPOINT = os.getenv("REQUESTS_ENDPOINT")


def convert_from_state_plane(df):
    """
    Adds a WGS-84 lat/long column to the dataframe based on the state plane coordinates.
    """
    # projection of coordinates
    transformer = Transformer.from_crs(crs_from="ESRI:102739", crs_to="EPSG:4326")
    df["latitude"], df["longitude"] = transformer.transform(
        df["State Plane X Coordinate"].tolist(), df["State Plane Y Coordinate"].tolist()
    )
    # Create wgs84 location columns in socrata format
    df["location"] = df.apply(build_point_data, axis=1)
    return df


def build_point_data(row):
    """

    Parameters
    ----------
    row: dict of data from a pandas dataframe

    Returns
    -------
    None if there is no location data given, or point datatype formatted as expected by Socrata.

    """
    if pd.isna(row["longitude"]) or pd.isna(row["latitude"]):
        return None
    return f"POINT ({row['longitude']} {row['latitude']})"


def get_fiscal_year(row):
    """
    Returns the fiscal year based on the created date of the record
    """
    year = row["datetime"].year
    month = row["datetime"].month
    if month >= 10:
        fiscal_year = year + 1
    else:
        fiscal_year = year

    return fiscal_year


def transform(df):
    logger.info("Transforming 311 Request data")
    df = convert_from_state_plane(df)

    # Converting datetime to correct format for socrata
    date_cols = [
        "Status Change Date",
        "Created Date",
        "Overdue On Date",
        "Last Update Date",
        "Close Date",
    ]

    # Generating fiscal year column based on the created date.
    df["datetime"] = pd.to_datetime(df["Created Date"])
    df["fiscal_year"] = df.apply(get_fiscal_year, axis=1)

    # Field mapping
    df = df[list(REQUESTS_MAP.keys())]
    df.rename(columns=REQUESTS_MAP, inplace=True)

    # date column formatting to match format expected by Socrata
    df = utils.transform_datetime_formats(df)

    # Setting these NaN values to None, because Socrata will be mad otherwise
    df["state_plane_x_coordinate"] = df["state_plane_x_coordinate"].replace(
        {np.nan: None}
    )
    df["state_plane_y_coordinate"] = df["state_plane_y_coordinate"].replace(
        {np.nan: None}
    )
    df["sr_location"] = df["sr_location"].replace({np.nan: None})

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

    logger.info("Uploading 311 request records to Socrata")
    res = utils.load_to_socrata(client=soda, dataset_id=DATASET, data=data)
    logger.info(res)

    return res


if __name__ == "__main__":
    logger = utils.get_logger(
        __name__,
        level=logging.INFO,
    )
    main()
