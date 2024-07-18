import io
import json
from datetime import timedelta
from urllib.parse import quote
import pandas as pd
import requests
from config import DB_CONN_STR, API_SATELLITE_ENDPOINT
from sqlalchemy import create_engine, exc

def get_polygons_from_land_info():
    '''
    Read polygons from land_info table in the database.

    Returns:
        DataFrame: The polygons read from the land_info table.
    '''
    with create_engine(DB_CONN_STR).connect() as conn_taft:
        try:
            print('Now is going to get the land info from DB ...')
            land_info = pd.read_sql(
                '''
                    SELECT i.land_id, d.index_name, MAX(d.index_date)  As max_index_date, i.coordinates, i.geometry_type 
                    FROM land_info AS i
                    LEFT JOIN land_satellite_index_data AS d
                    ON i.land_id = d.land_id
                    GROUP BY i.land_id , d.index_name
                ''',
                conn_taft
            )
            print(f"There are {land_info.shape[0]} land info records fetched.")
            return land_info
        except exc.SQLAlchemyError as req_err_msg:
            print(f"An error occurred while writing data into DB:{req_err_msg}")
            conn_taft.rollback()
            return None


def generate_geojson_for_request_body(land_info_row):
    '''
    Generate GeoJSON for the request body of the API.

    Args:
        land_info (DataFrame): The land information data.

    Returns:
        dict: The GeoJSON for the request body of the API.
    '''
    print('Now is going to generate GeoJSON for the request body of the API ...')
    
    
    if land_info_row['coordinates'] == None:
        return
    geojson = {
        "type": "FeatureCollection",
        "name": "datayoo",
        "crs": {
            "type": "name",
            "properties": {
                "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
            }
        },
        "features": [{
            "type": "Feature",
            "properties": {"field_id": land_info_row['land_id']},
            "geometry": {
                "type": land_info_row['geometry_type'],
                "coordinates": eval(land_info_row['coordinates'])
            }
        }]
    }

    return geojson


def date_add_one_day(date):
    '''
    Add one day to the date string.

    Args:
        date_str (str): The date string.

    Returns:
        str: The date string after adding one day.
    '''
    date += timedelta(days=1)
    return date.strftime("%Y-%m-%d")


def get_satellite_data_from_api(geojson, index_name, last_time_update):
    '''
    Get satellite data from the API.

    Args:
        geojson (dict): The GeoJSON for the request body of the API.

    Returns:
        dict: The satellite data from the API.
    '''

    params = {
        "token": "z3tw3rm6e5DxA2aY",
        "index_name": index_name,
        "start_time": last_time_update,
        "data_source": "sen2",
    }

    url = f'{API_SATELLITE_ENDPOINT}/33FieldAvgCropIndex?'
    print(f"url: {url}")

    try:
        print('Getting the satellite data from API ...' +
              f'{index_name} {last_time_update}')
        with io.BytesIO(json.dumps(geojson).encode('utf-8')) as json_bytes:
            files = {"field_info": (
                '', json_bytes, 'application/octet-stream')}
            response = requests.post(url=url, files=files, params=params)

        if response.status_code == 200:
            print(f'{index_name} {last_time_update} data is fetched successfully.')
            try:
                if response.json()["result"] == []:
                    return pd.DataFrame()
            except:
                print(f"An error occurred while fetching data from the API: \
                    {response.text}")
                return pd.DataFrame()
            
            response_df = pd.DataFrame(response.json()["result"]).rename(
                columns={'index': 'index_value', 'time': 'index_date'})
            response_df.drop(['field_id', 'cloud'], axis=1, inplace=True)
            return response_df
        else:
            print(f"An error occurred while fetching data from the API: \
                {response.text}")
            return None
    except requests.exceptions.RequestException as req_err_msg:
        print(f"An error occurred while fetching data from the API: \
            {req_err_msg}")
        return None


def land_satellite_crawler():
    """
    Fetches land information data, processes it, and writes it into the database.

    This function performs the following steps:
    1. Fetches trace codes that exist in resume land info but missing land information from the database.
    2. Fetches land information data based on the trace codes.
    3. Processes and inserts land information data into the database.
    """
    # # Fetch land information data
    land_info = get_polygons_from_land_info()
    land_info_for_iter = land_info.copy()
    land_info_for_iter = land_info_for_iter.drop_duplicates(subset=['land_id'])
    print(f"land_info_for_iter: {land_info_for_iter}")

    # Get satellite data from the API
    print('Now is going to get the satellite data from the API ...')

    with create_engine(DB_CONN_STR).connect() as conn_taft:
        try:
            for index, row in land_info_for_iter.iterrows():
                if row['coordinates'] == None:
                    continue
                geojson = generate_geojson_for_request_body(row)
                curr_land_id = row['land_id']
                for index_i in ["NDRE", "NDVI", "NDWI", "NDWI2"]:
                    print("-----------------------------------")
                    print(f"curr_land_id: {curr_land_id}, index_i: {index_i}")
                    index_date = land_info[(land_info['land_id'] == curr_land_id) & (
                        land_info['index_name'] == index_i)]['max_index_date']
                    if index_date.empty == False:
                        result = get_satellite_data_from_api(
                            geojson, index_i, date_add_one_day(index_date.iloc[0]))
                    else:
                        result = get_satellite_data_from_api(
                            geojson, index_i, "")

                    # add data rows in result to satellite_df
                    if result.empty == False:
                        result['land_id'] = curr_land_id
                        result.to_sql('land_satellite_index_data',
                                      con=conn_taft, if_exists='append', index=False)
                        conn_taft.commit()
        except exc.SQLAlchemyError as req_err_msg:
            print(f"An error occurred while writing data into DB: \
                {req_err_msg}")
            conn_taft.rollback()

if __name__ == '__main__':
    land_satellite_crawler()