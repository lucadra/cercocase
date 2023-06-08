import json
import pandas as pd
import requests
import pathlib
import time
import tqdm
import concurrent.futures
import logging

LISTINGS_ENDPOINT = "https://www.immobiliare.it/api-next/search-list/real-estates/"
ID_CONTRATTO = 1  # 1 = SALES, 2 = RENTALS


def generate_payloads(index: dict) -> dict:
    """
    Generate API payloads based on the information contained in a CSV file.

    Args:
        row (pd.Series): A pandas series containing the data.

    Returns:
        dict: An API payload.
    """
    payload = {
        "fkRegione": index["region_id"],
        "idProvincia": index["province_id"],
        "idComune": int(index["city_id"]),
        "idNazione": "IT",
        "idContratto": ID_CONTRATTO,
        "idCategoria": 1,
        "criterio": "rilevanza",
        "noAste": 1,
        "__lang": "it",
        "pag": index.get("page_num", 0),
        "paramsCount": 1,
        "path": "%2F",
    }

    if index["macrozone_id"] != 0:
        payload.update(
            {
                "idMZona[0]": int(index["macrozone_id"]),
                "idQuartiere[0]": int(index["neighbourhood_id"]),
            }
        )

    return payload


def get_data(row: pd.Series, session: requests.Session) -> list:
    response = session.get(LISTINGS_ENDPOINT, params=generate_payloads(row))
    try:
        data = response.json()
        max_pages = data.get("maxPages", 0)
        indexes = []
        for page in range(1, max_pages + 1):
            indexes.append(
                {
                    "region_id": row.get("region_id", None),
                    "province_id": row.get("province_id", None),
                    "city_id": row.get("city_id", None),
                    "macrozone_id": row.get("macrozone_id", None),
                    "neighbourhood_id": row.get("neighbourhood_id", None),
                    "page_num": page,
                }
            )
        return indexes
    except json.decoder.JSONDecodeError:
        return (row["city_name"], row["macrozone_name"])


def build_indexes(macrozone_df: pd.DataFrame) -> list:
    indexes = []
    errors = []
    with requests.Session() as session:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(get_data, row, session): row
                for _, row in macrozone_df.iterrows()
            }
            for future in tqdm.tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc="Building indexes",
                smoothing=0.05,
            ):
                result = future.result()
                if isinstance(result, list):
                    indexes.extend(result)
                else:
                    errors.append(result)

    if errors:
        logging.info(f"Could not parse {len(errors)} macrozones:")
        for error in errors:
            logging.info(f"{error[0]} {error[1]}")

    return indexes


def download_listings_page(
    index: dict, session: requests.Session, save_path: pathlib.Path
) -> None:
    response = session.get(LISTINGS_ENDPOINT, params=generate_payloads(index))
    with open(
        save_path
        / f"{index['region_id']}_{index['province_id']}_{index['city_id']}_{index['macrozone_id']}_{index['neighbourhood_id']}_{index['page_num']}.json",
        "w",
    ) as f:
        json.dump(response.json(), f)


def download_listings(indexes: list) -> None:
    save_path = pathlib.Path(f"./listings/{time.strftime('%y%m%d')}/json/")
    save_path.mkdir(parents=True, exist_ok=True)

    with requests.Session() as session:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(download_listings_page, index, session, save_path)
                for index in indexes
            ]
            for future in tqdm.tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc="Downloading listings",
                smoothing=0.05,
            ):
                try:
                    future.result()
                except Exception as e:
                    print(f"Exception occurred in worker thread: {e}")


if __name__ == "__main__":
    pass
