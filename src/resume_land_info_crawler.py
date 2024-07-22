import json
import time
import urllib.parse
from urllib.parse import quote

import pandas as pd
import requests
from config import DB_CONN_STR
from sqlalchemy import create_engine, exc

def get_section_info() -> pd.DataFrame:
    """
    Fetches section information data from the API.

    Returns:
        pd.DataFrame: A DataFrame containing the fetched section information data containing the following columns:
            - unit_id
            - section_id
            - section_name
            - county_name
            - town_name
    """
    username = "datayoo"
    password = urllib.parse.quote_plus("*@(!)@&#")
    dbname = "land"
    port = "3306"
    host = "192.168.1.103"
    db_conn_land = f"mysql+pymysql://{username}:{password}@{host}:{port}/{dbname}"
    with create_engine(db_conn_land).connect() as conn_taft:
        try:
            print('Now is going to get section info from DB ...')
            section_info = pd.read_sql(
                '''
                select county_name, town_name, unit_id, section_id, section_name 
                from land.section_info;
                ''',
                conn_taft
            )
            print(f"There are {section_info.shape[0]} section info records fetched.")
            return section_info
        except exc.SQLAlchemyError as req_err_msg:
            print(f"An error occurred while writing data into DB: {req_err_msg}")
            conn_taft.rollback()


def preprocess_land_serial_no(resume_land_info_df: pd.DataFrame, section_info_df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses the land serial number data.

    Args:
        resume_land_info_df (pd.DataFrame): The resume land info DataFrame containing the following columns:
            - unit_id
            - section_id
            - full_land_no

        section_info_df (pd.DataFrame): The section info DataFrame containing the following columns:

    Returns:
        pd.DataFrame: A DataFrame containing the preprocessed land serial number data containing the following columns:
    """
    print('Now is going to preprocess the land serial number ...')
    unknown_lands_processed = pd.merge(resume_land_info_df, section_info_df, on=[
                                       'unit_id', 'section_id'], how='left')
    unknown_lands_processed['parcel_1'] = unknown_lands_processed['full_land_no'].str.slice(
        0, 4).apply(lambda x: int(x) if x.isdigit() else '').astype(str)
    unknown_lands_processed['parcel_2'] = unknown_lands_processed['full_land_no'].str.slice(
        4, 8).apply(lambda x: int(x) if x.isdigit() else '').astype(str)
    unknown_lands_processed['land_serial_no'] = unknown_lands_processed['county_name'] + unknown_lands_processed['town_name'] + \
        unknown_lands_processed['section_name'] + unknown_lands_processed['parcel_1'] + \
        '-' + unknown_lands_processed['parcel_2'] + '地號'
    unknown_lands_processed['land_serial_no'] = unknown_lands_processed['land_serial_no'].str.replace(
        '-0', '')
    unknown_lands_processed = unknown_lands_processed.query(
        'land_serial_no.str.contains("NA") == False')
    unknown_lands_processed = unknown_lands_processed.query(
        'county_name not in ["澎湖縣", "金門縣", "連江縣"]')
    unknown_lands_processed.reset_index(drop=True, inplace=True)
    return unknown_lands_processed

def determine_geometry_type(coordinates: str) -> str:
    """
    Determine the geometry type based on the given coordinates.

    Args:
        coordinates (str): The coordinates.

    Returns:
        str: The geometry type.
    """
    if coordinates is None:
        return None
    
    if coordinates.startswith("[[[["):
        return "MultiPolygon"
    elif coordinates.startswith("[[["):
        return "Polygon"
    elif coordinates.startswith("[["):
        return "LineString"
    elif coordinates.startswith("["):
        return "Point"
    else:
        return None

def get_land_serial_no_geometry(land_serial_no, land_version='112Oct'):
    '''
    Get the land info by land_serial_no from coagis.colife.org.tw

    Args:   
        land_serial_no (str): The land_serial_no of the land.
        land_version (str): The land_version of the land.

    Returns:
        dict: The land info of the land.
    '''
    request_headers = {
        "Host": "coagis.colife.org.tw",
        "Origin": "https://map.moa.gov.tw",
        "Referer": "https://map.moa.gov.tw/"
    }

    coa_token = 'sRwtJZ5mdzMWMzlReMnaRdRQabgGgnHu4zDtSY7JkVbgMRprP-dkv-84cczetfl4YLck-rQmvCIlxiyYoFdxIg..'
    land_serial_no_encoded = quote(land_serial_no)

    request_url = (
        'https://coagis.colife.org.tw/arcgis/rest/services/CadastralMap/SOE/MapServer/exts/CoaRESTSOE/LandAddressToLocation_ring?'
        f'token={coa_token}&'
        f'LandAddress={land_serial_no_encoded}&'
        f'LandVersion={land_version}&'
        f'CodeVersion={land_version}&'
        f'SpatialRefZone={quote("本島")}&'
        'SpatialRefOutput=4326&'
        'f=json'
    )

    response = requests.get(request_url, headers=request_headers)
    if response.status_code == 200:
        print(response.json()['ReturnDescription'])
        if "接近" in response.json()['ReturnDescription']:
            print(f"Failed to fetch land info of land_serial_no: {land_serial_no}")
            return None
        print(f"Successfully fetched land info of land_serial_no: {land_serial_no}")
        try:
            return json.loads(response.json()['ReturnResult'][0]['ReturnPolygon'])['rings']
        except Exception as e:
            print(f"An error occurred while fetching land info of land_serial_no: {land_serial_no} - {e}")
            return None
    else:
        response.raise_for_status()

def get_s_n_id_geometry(unit_id, section_id, full_land_no):
    '''
    Get the land info by unit_id, section_id, full_land_no from taft.moa.gov.tw

    Args:
        unit_id (str): The unit_id of the land.
        section_id (str): The section_id of the land.
        full_land_no (str): The full_land_no of the land.

    Returns:
        list: The coordinates of the land.
    '''
    request_headers = {
        "Host": "taft.moa.gov.tw",
        "Origin": "https://taft.moa.gov.tw"
    }

    response = requests.post(
        'https://taft.moa.gov.tw/sp-resume-service-1.html',
        data={
            "action": "Geo_OP",
            "SID": f"{unit_id}{section_id}",
            "NID": full_land_no
        },
        headers=request_headers
    )
    try:
        response_content = response.json()
        if response_content['success']:
            print(f"Successfully fetched land info of trace code: {full_land_no}")
            return response_content['data'][0]['geometry']['rings']
        else:
            print(f"Failed to fetch land info of SID: {unit_id}{section_id} NID: {full_land_no}")
            return None
    # print(response_content)
    except Exception as e:
        print(f"An error occurred while fetching land info of SID: {unit_id}{section_id} NID: {full_land_no} - {e}")
        return None

    


def land_no_convert(unit_id, section_id, full_land_no):
    request_headers = {
        "Host": "openapi.land.moi.gov.tw",
        "Origin": "https://cop.land.moi.gov.tw",
    }
    print(f"Start to convert land_no: {full_land_no} ...")
    try:
        response = requests.post(
            'https://openapi.land.moi.gov.tw/WEBAPI/LandNewOldQuery/1.0/QueryLandNo',
            json=[{
                "UNIT": unit_id,
                "SEC": section_id,
                "NO": full_land_no,
                "TYPE": "N"
            }],
            headers=request_headers
        )
    except Exception as e:
        print(f"An error occurred while converting land_no: {full_land_no} - {e}")
        return None

    response_content = response.json()

    return response_content


def resume_land_info_crawler() -> None:
    """
    Fetches land information data, processes it, and writes it into the database.

    This function performs the following steps:
    1. Fetches trace codes that exist in resume land info but missing land information from the database.
    2. Fetches land information data based on the trace codes.
    3. Processes and inserts land information data into the database.
    """
    with create_engine(DB_CONN_STR).connect() as conn_taft:
        try:
            print('Now is going to get the trace code list lack of land info from DB ...')
            missing_land_info = pd.read_sql(
                '''
                select distinct resume_land_info.unit_id, resume_land_info.section_id, resume_land_info.full_land_no 
                from resume_land_info left join land_info on
                resume_land_info.unit_id = land_info.unit_id and
                resume_land_info.section_id = land_info.section_id and
                resume_land_info.full_land_no = land_info.full_land_no
                where land_info.land_id is null;
                ''',
                conn_taft
            )
            print(f"There are {missing_land_info.shape[0]} trace code records is missing land information.")

        except exc.SQLAlchemyError as req_err_msg:
            print(f"An error occurred while writing data into DB: {req_err_msg}")
            conn_taft.rollback()

    unknown_lands = preprocess_land_serial_no(
        missing_land_info, get_section_info())
    print(f"There are {unknown_lands.shape[0]} unknown land serial number records fetched.")
    total_update_records = 0

    unknown_lands["coordinates"] = None

    for index, row in unknown_lands.iterrows():
        time.sleep(1)
        print("--------------------------------------------------")
        print(f"Processing record {index + 1} of {unknown_lands.shape[0]} ...")
        print("->")
        land_info = get_s_n_id_geometry(
            row['unit_id'], row['section_id'], row['full_land_no'])
        if land_info is not None:
            row["coordinates"] = land_info
            continue

        # # transform land_no to new one
        new_land_no = land_no_convert(
            row['unit_id'], row['section_id'], row['full_land_no'])
        if new_land_no["RETURNROWS"] != 0:
            land_info = get_s_n_id_geometry(
                new_land_no["RESPONSE"][0]["UNIT"], new_land_no["RESPONSE"][0]["SEC"], new_land_no["RESPONSE"][0]["NO"])
            if land_info is not None:
                row["coordinates"] = land_info
                continue
        
    for index, row in unknown_lands.iterrows():
        # fetch land info by land_serial_no
        land_serial_no = row['land_serial_no']
        land_info = get_land_serial_no_geometry(land_serial_no)
        if land_info is not None:
            row["coordinates"] = land_info

    unknown_lands['geometry_type'] = unknown_lands['coordinates'].apply(
        determine_geometry_type)
    
    
    # remove unnecessary columns
    unknown_lands['coordinates'] = unknown_lands['coordinates'].apply(
        lambda x: str(x) if x is not None else None)
    unknown_lands.drop(columns=['county_name', 'town_name',
                       'section_name', 'parcel_1', 'parcel_2'], inplace=True)

    with create_engine(DB_CONN_STR).connect() as conn_taft:
        try:
            print('Now is going to insert the land info into DB ...')
            unknown_lands.to_sql('land_info', con=conn_taft,
                                 if_exists='append', index=False)
            total_update_records = unknown_lands.shape[0]
            conn_taft.commit()
        except exc.SQLAlchemyError as req_err_msg:
            print(f"An error occurred while writing data into DB: {req_err_msg}")
            conn_taft.rollback()

    print(f"All update is done! There are {total_update_records} record(s) inserted in total.")

if __name__ == '__main__':
    resume_land_info_crawler()