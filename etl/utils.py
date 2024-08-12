import logging
import sys
import pandas as pd


def get_logger(name, level):
    """Return a module logger that streams to stdout"""
    logger = logging.getLogger(name)
    formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s: %(message)s")
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)
    return logger


def extract(endpoint, logger, sep="\t", encoding="utf_16"):
    """
    Extract data from CSR report endpoint. Some reports are Excel, others are csv.
    Sep, encoding are only used if the endpoint is a csv report.
    """
    try:
        if ".csv" in endpoint.lower():
            df = pd.read_csv(endpoint, sep=sep, encoding=encoding)
        else:
            df = pd.read_excel(endpoint)
    except UnicodeError as e:
        logger.info(
            "Unexpected file type returned from the report endpoint. Check that you are on the city network. "
            "It's likely that your request is getting flagged as a bot by the web app firewall."
        )
        raise e
    except Exception as e:
        raise e
    logger.info(f"Downloaded {len(df)} records from endpoint")
    return df


def transform_datetime_formats(df):
    """
    Changes the format of all columns in the dataframe which contain "date" to match the format expected by Socrata
    """
    for col in df.columns:
        if "date" in col:
            df[col] = pd.to_datetime(df[col])
            df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S")
            df[col] = df[col].where(pd.notnull(df[col]), None)
    return df


def load_to_socrata(client, dataset_id, data, method="upsert"):
    """
    Loads data into a Socrata dataset either replacing all records, or upserting records.
    """
    assert method in ["upsert", "replace"]
    if method == "upsert":
        res = client.upsert(dataset_id, data)
    elif method == "replace":
        res = client.replace(dataset_id, data)
    return res
