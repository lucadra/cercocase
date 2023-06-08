import json
import pandas as pd
import pathlib
import time
import re
import tqdm


def parse_result(result: dict) -> dict:
    real_estate = result.get("realEstate", {})
    properties = real_estate.get("properties", [{}])[0]
    location = properties.get("location", {})
    price = real_estate.get("price", {})
    floor_info = properties.get("floor", {})
    typology = properties.get("typology", {})

    id = real_estate.get("id")
    city = location.get("city")
    macrozone = location.get("macrozone")
    neighbourhood = location.get("microzone")
    price_value = price.get("value")
    surface_string = properties.get("surface")
    rooms_string = properties.get("rooms")
    floor = floor_info.get("abbreviation")
    type = typology.get("name")

    surface = (
        int(re.search(r"-?\d+(\.\d+)?", str(surface_string)).group().replace(".", ""))
        if surface_string
        else None
    )
    rooms = (
        int(re.search(r"-?\d+(\.\d+)?", str(rooms_string)).group())
        if rooms_string
        else None
    )

    price_per_sqm = price_value / surface if price_value and surface else None

    return {
        "id": id,
        "city": city,
        "macrozone": macrozone,
        "neighbourhood": neighbourhood,
        "price": price_value,
        "price_per_sqm": price_per_sqm,
        "surface": surface,
        "rooms": rooms,
        "floor": floor,
        "type": type,
    }


def parse_listings_page(file_path: pathlib.Path) -> pd.DataFrame:
    with open(file_path, "r") as f:
        try:
            data = json.load(f)
        except json.decoder.JSONDecodeError:
            print(f"Could not parse {file_path}")
            return pd.DataFrame()

        results = data.get("results", [])
        parsed_results = [parse_result(result) for result in results]
        df = pd.DataFrame(parsed_results)
        return df


def compile_city_tables() -> None:
    json_path = pathlib.Path(f"./listings/{time.strftime('%y%m%d')}/json/")
    json_files = [path for path in json_path.glob("*.json")]
    indexes = {path.stem[:6] for path in json_files}
    save_path = pathlib.Path(f"./listings/{time.strftime('%y%m%d')}/csv/")
    save_path.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame()
    for index in tqdm.tqdm(indexes, desc="Compiling city tables"):
        json_files = [
            path for path in json_path.glob("*.json") if path.stem[:6] == index
        ]

        dfs = [parse_listings_page(json_file) for json_file in json_files]
        df = pd.concat(dfs)
    df = df.dropna(subset=["price", "surface"])
    df.to_csv(save_path / f"{index}.csv", index=False)


def compile_macrozone_summary_table():
    csv_path = pathlib.Path(f"./listings/{time.strftime('%y%m%d')}/csv/")
    save_path = pathlib.Path(f"./listings/{time.strftime('%y%m%d')}/out/")
    save_path.mkdir(parents=True, exist_ok=True)
    macrozone_summary = pd.DataFrame(
        columns=[
            "city_name",
            "macrozone_name",
            "price_mean",
            "price_median",
            "price_std",
            "price_min",
            "price_max",
            "price_q50",
            "price_q90",
            "price_per_sqm_mean",
            "price_per_sqm_median",
            "price_per_sqm_std",
            "price_per_sqm_min",
            "price_per_sqm_max",
            "price_per_sqm_q50",
            "price_per_sqm_q90",
            "surface_mean",
            "surface_median",
            "surface_std",
            "surface_min",
            "surface_max",
            "surface_q50",
            "surface_q90",
        ]
    )

    def q50(x):
        return x.quantile(0.5)

    def q90(x):
        return x.quantile(0.9)

    for csv_file in tqdm.tqdm(
        csv_path.glob("*.csv"),
        desc="Compiling macrozone summary table",
        smoothing=0.05,
        total=len(list(csv_path.glob("*.csv"))),
    ):
        city_data = pd.read_csv(csv_file)
        city_data = city_data.dropna(subset=["price", "surface"])
        city_name = city_data["city"].iloc[0]

        macrozone_data = city_data.groupby("macrozone").agg(
            {
                "price": ["mean", "median", "std", "min", "max", q50, q90],
                "price_per_sqm": ["mean", "median", "std", "min", "max", q50, q90],
                "surface": ["mean", "median", "std", "min", "max", q50, q90],
            }
        )

        for macrozone, data in macrozone_data.iterrows():
            macrozone_summary = macrozone_summary._append(
                {
                    "city_name": city_name,
                    "macrozone_name": macrozone,
                    "price_mean": data["price"]["mean"],
                    "price_median": data["price"]["median"],
                    "price_std": data["price"]["std"],
                    "price_min": data["price"]["min"],
                    "price_max": data["price"]["max"],
                    "price_q50": data["price"]["q50"],
                    "price_q90": data["price"]["q90"],
                    "price_per_sqm_mean": data["price_per_sqm"]["mean"],
                    "price_per_sqm_median": data["price_per_sqm"]["median"],
                    "price_per_sqm_std": data["price_per_sqm"]["std"],
                    "price_per_sqm_min": data["price_per_sqm"]["min"],
                    "price_per_sqm_max": data["price_per_sqm"]["max"],
                    "price_per_sqm_q50": data["price_per_sqm"]["q50"],
                    "price_per_sqm_q90": data["price_per_sqm"]["q90"],
                    "surface_mean": data["surface"]["mean"],
                    "surface_median": data["surface"]["median"],
                    "surface_std": data["surface"]["std"],
                    "surface_min": data["surface"]["min"],
                    "surface_max": data["surface"]["max"],
                    "surface_q50": data["surface"]["q50"],
                    "surface_q90": data["surface"]["q90"],
                },
                ignore_index=True,
            )

    macrozone_summary = macrozone_summary.round(2)
    macrozone_summary.to_csv(
        f"./listings/{time.strftime('%y%m%d')}/out/summary_table.csv", index=False
    )

    return macrozone_summary


if __name__ == "__main__":
    compile_city_tables()
    compile_macrozone_summary_table()
