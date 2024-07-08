from urllib.parse import quote
import urllib.parse
import requests
import pandas as pd
from sqlalchemy import create_engine, exc
from sqlalchemy.orm import Session
from config import load_config
import time
import json
import io
from datetime import datetime, timedelta

# 加载配置并存储在变量中``
config = load_config()

# Define Variables
DB_US = config['db_us']
DB_PW = config['db_pw']
DB_HT = config['db_ht']
DB_PORT = config['db_port']
DB_NAME = config['db_name']
DB_CONN_STR = config['db_conn_str']

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
                    SELECT i.land_id, d.index_name, MAX(d.index_date)  As max_index_date, i.coordinates
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
            print(f"An error occurred while writing data into DB: {req_err_msg}")
            conn_taft.rollback()
            return None

def generate_geojson_for_request_body(land_info):
    '''
    Generate GeoJSON for the request body of the API.
    
    Args:
        land_info (DataFrame): The land information data.
        
    Returns:
        dict: The GeoJSON for the request body of the API.
    '''
    print('Now is going to generate GeoJSON for the request body of the API ...')

    total = 0
    geojsons = {}
    for index, row in land_info.iterrows():
        for index_i in ["NDRE", "NDVI", "NDWI", "NDWI2"]:
            if row['coordinates'] == None:
                continue
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
                    "properties": {"field_id": row['land_id']},
                    "geometry": {
                        "type": "MultiPolygon",
                        "coordinates": eval(row['coordinates'])
                    }
                }]
            }
            total += 1
            geojsons[(row['land_id'], index_i)] = geojson

    print(f"Total {total} GeoJSONs are generated.")  # should be 4 * land_info_for_iter.shape[0]
    return geojsons

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
    
    url = 'http://192.168.1.104:8886/33FieldAvgCropIndex?'
    
    
    try:
        print(f'Getting the satellite data from API ...{index_name} {last_time_update}')
        with io.BytesIO(json.dumps(geojson).encode('utf-8')) as json_bytes:
            files = {"field_info": ('', json_bytes, 'application/octet-stream')}
            response = requests.post(url=url, files=files, params=params)
        
        if response.status_code == 200:
            print(f'{index_name} {last_time_update} data is fetched successfully.')
            if response.json()["result"] == []:
                return pd.DataFrame()
            response_df = pd.DataFrame(response.json()["result"]).rename(columns={'index': 'index_value', 'time': 'index_date'})
            response_df.drop(['field_id', 'cloud'], axis=1, inplace=True)
            return response_df
        else:
            print(f"An error occurred while fetching data from the API: {response.text}")
            return None
    except requests.exceptions.RequestException as req_err_msg:
        print(f"An error occurred while fetching data from the API: {req_err_msg}")
        return None

def main() -> None:
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
    
    # # Generate GeoJSON for the request body of the API
    geojsons = generate_geojson_for_request_body(land_info_for_iter)
    
    # Get satellite data from the API
    print('Now is going to get the satellite data from the API ...')
    engine = create_engine(DB_CONN_STR)
    
    with Session(engine) as session:
        try:
            for index, row in land_info_for_iter.iterrows():
                if row['coordinates'] == None:
                    continue
                curr_land_id = row['land_id']
                for index_i in ["NDRE", "NDVI", "NDWI", "NDWI2"]:
                    print("-----------------------------------")
                    print(f"curr_land_id: {curr_land_id}, index_i: {index_i}")
                    index_date = land_info[(land_info['land_id'] == curr_land_id) & (land_info['index_name'] == index_i)]['max_index_date']
                    
                    print(f"index_date: {index_date}")
                    if index_date.empty == False:
                        result = get_satellite_data_from_api(geojsons[(curr_land_id, index_i)], index_i, date_add_one_day(index_date.iloc[0]))
                    else:
                        result = get_satellite_data_from_api(geojsons[(curr_land_id, index_i)], index_i, "")
                    
                    # add data rows in result to satellite_df
                    if result.empty == False:
                        result['land_id'] = curr_land_id
                        result.to_sql('land_satellite_index_data', con=engine, if_exists='append', index=False)
            
            session.commit()
        except exc.SQLAlchemyError as req_err_msg:
            print(f"An error occurred while writing data into DB: {req_err_msg}")       
            session.rollback()         

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"An error occurred: {e}")