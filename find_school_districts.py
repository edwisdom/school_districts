import googlemaps
import geopandas as gpd
from geopandas import GeoDataFrame
import pandas as pd
from shapely.geometry import Point

import os
from pathlib import Path


def create_gmaps_client() -> googlemaps.Client:
    """Creates a Google Maps client using the API key from environment variables.

    Returns:
        googlemaps.Client: A client for interacting with the Google Maps API.
    """
    api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
    return googlemaps.Client(key=api_key)


def get_coordinate(client: googlemaps.Client, address: str) -> Point:
    """Retrieves the geographical coordinates (longitude, latitude) for a given address.

    Args:
        client (googlemaps.Client): The Google Maps client.
        address (str): The address to geocode.

    Returns:
        Point: A Point object representing the coordinates of the address. Returns None if the address cannot be geocoded.
    """
    geocode_result = client.geocode(address)

    if (
        geocode_result is None
        or "geometry" not in geocode_result[0]
        or "location" not in geocode_result[0]["geometry"]
    ):
        return None

    location = geocode_result[0]["geometry"]["location"]
    return Point(location["lng"], location["lat"])


def read_shapefile(path: Path, state_fip: str = "04") -> GeoDataFrame:
    """Reads a shapefile and filters it by the given state FIP code.

    Args:
        path (Path): The path to the shapefile.
        state_fip (str, optional): The state FIP code to filter by.
            Defaults to "04" for Arizona.

    Returns:
        GeoDataFrame: A GeoDataFrame containing the filtered shapefile data.
    """
    gdf = gpd.read_file(path)
    state_gdf = gdf[gdf["STATEFP"] == state_fip]
    state_gdf["LOGRADE"] = (
        pd.to_numeric(state_gdf["LOGRADE"], errors="coerce").fillna(0).astype(int)
    )
    state_gdf["HIGRADE"] = (
        pd.to_numeric(state_gdf["HIGRADE"], errors="coerce").fillna(0).astype(int)
    )
    return GeoDataFrame(
        state_gdf[["NAME", "geometry", "LOGRADE", "HIGRADE"]], geometry="geometry"
    )


def get_all_coordinates(df: pd.DataFrame) -> GeoDataFrame:
    """Gets the geographical coordinates for all addresses in the DataFrame.

    Args:
        df (pd.DataFrame): A DataFrame containing address information.

    Returns:
        GeoDataFrame: A GeoDataFrame containing the coordinates and grade information.
    """
    client = create_gmaps_client()
    df["FULL_ADDRESS"] = df[["PSS_ADDRESS", "PSS_CITY", "PSS_STABB"]].agg(
        ",".join, axis=1
    )
    df["COORDINATE"] = df["FULL_ADDRESS"].apply(
        lambda address: get_coordinate(client, address)
    )
    return GeoDataFrame(df[["COORDINATE", "LOGRADE", "HIGRADE"]], geometry="COORDINATE")


def get_districts_for_coordinates(
    coords_gdf: GeoDataFrame, districts: GeoDataFrame
) -> pd.Series:
    """Determines the school districts for each set of coordinates.

    Args:
        coords_gdf (GeoDataFrame): A GeoDataFrame containing coordinates.
        districts (GeoDataFrame): A GeoDataFrame containing district boundaries.

    Returns:
        pd.Series: A Series containing the districts for each coordinate.
    """
    joined_gdf = gpd.sjoin(
        coords_gdf, districts, predicate="within", lsuffix="school", rsuffix="district"
    )
    grade_filtered_gdf = joined_gdf[
        (joined_gdf["LOGRADE_school"] >= joined_gdf["LOGRADE_district"])
        | (joined_gdf["HIGRADE_school"] <= joined_gdf["HIGRADE_district"])
    ]
    district_series = grade_filtered_gdf.groupby(level=0)["NAME"].agg(list)
    return district_series


def get_districts_for_schools(
    schools_df: pd.DataFrame,
    districts_gdf: GeoDataFrame,
    coords_path: Path,
    districts_path: Path,
) -> None:
    """
    First, gets coordinates for each school, and then gets the districts for each.
    Saves the results from both steps to separate CSV files.

    Args:
        schools_df (pd.DataFrame): A DataFrame containing school information.
        districts_gdf (GeoDataFrame): A GeoDataFrame containing district boundaries.
        coords_path (Path): The path to save the coordinates CSV file.
        districts_path (Path): The path to save the districts CSV file.
    """
    # Get coordinates for each school and save
    print("Getting coordinates")
    coordinates = get_all_coordinates(schools_df)
    schools_with_coords = schools_df.assign(COORDINATE=coordinates["COORDINATE"])
    schools_with_coords.to_csv(coords_path)

    # Get districts for each coordinate and save
    print("Getting districts for each coordinate")
    districts = get_districts_for_coordinates(
        coords_gdf=coordinates, districts=districts_gdf
    )
    schools_with_districts = schools_with_coords.assign(DISTRICTS=districts)
    schools_with_districts.to_csv(districts_path)


if __name__ == "__main__":
    DATA_DIR = Path("data")
    DISTRICT_SHAPEFILE_PATH = DATA_DIR / Path(
        "us_school_districts/EDGE_SCHOOLDISTRICT_TL_23_SY2223.shp"
    )
    SCHOOLS_PATH = DATA_DIR / Path("school_data.csv")
    COORDS_PATH = DATA_DIR / Path("schools_with_coords.csv")
    DISTRICTS_PATH = DATA_DIR / Path("schools_with_districts.csv")

    print("Reading in schools")
    schools_df = pd.read_csv(SCHOOLS_PATH).rename(columns=str.upper)

    print("Reading in district shapefiles")
    districts_gdf = read_shapefile(DISTRICT_SHAPEFILE_PATH)

    get_districts_for_schools(
        schools_df=schools_df,
        districts_gdf=districts_gdf,
        coords_path=COORDS_PATH,
        districts_path=DISTRICTS_PATH,
    )
