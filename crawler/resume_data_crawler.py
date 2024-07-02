import urllib.parse
import requests
import pandas as pd
from sqlalchemy import create_engine, exc
from sqlalchemy.orm import Session
from config import load_config

# 加载配置并存储在变量中
config = load_config()

# Define Variables
RESUME_DATA_API_ENDPOINT = config['resume_data_api_endpoint']
RESUME_DATA_REQ_MAXIMUM = int(config['resume_data_req_maximum'])
DB_US = config['db_us']
DB_PW = config['db_pw']
DB_HT = config['db_ht']
DB_PORT = config['db_port']
DB_NAME = config['db_name']
DB_CONN_STR = config['db_conn_str']

def fetch_resume_data(skip: int) -> pd.DataFrame:
    """
    Download and save resume data.

    Args:
        skip (int): The number of records to skip.

    Returns:
        pandas.DataFrame: The resume data as a DataFrame.
    """
    print("collecting taft resume data with params skip = " + str(skip))
    params = {"$top": 10000, "$skip": skip}
    response = requests.get(RESUME_DATA_API_ENDPOINT, params=params)
    response_content = response.json()
    response_df = pd.json_normalize(response_content)
    print("Data is retrieved successfully! There are " + str(response_df.shape[0]) + " row(s) returned in total.")
    print("UpdateTime: " + response_df["Log_UpdateTime"].iloc[0])
    response_df.rename(columns={"Tracecode": "TraceCode"}, inplace=True)
    return response_df

def fetch_and_process_resume_data(last_time_update: str) -> pd.DataFrame:
    """
    Fetches resume data from an external API, processes it, and returns a DataFrame.

    Returns:
        pandas.DataFrame: Processed resume data containing the following columns:
            - trace_code
            - product_name
            - org_id
            - org_name
            - farmer_name
            - pkg_date
            - store_info
            - sub_product_name
            - LandSecNO
    """

    print(f"Last time update in database: {last_time_update}")
    
    resume_data_list = []
    for skip_i in range(0, RESUME_DATA_REQ_MAXIMUM, 10000):
        new_fetched_resume_data = fetch_resume_data(skip_i)
        if last_time_update is not None:
            new_fetched_resume_data = new_fetched_resume_data[new_fetched_resume_data["Log_UpdateTime"] > last_time_update]
            
        print(f"New fetched resume data shape: {new_fetched_resume_data.shape[0]}")
        if new_fetched_resume_data.shape[0] != 10000:
            break
        resume_data_list.append(new_fetched_resume_data)
        
    
    # resume_data_list = [fetch_resume_data(skip_i) for skip_i in range(0, RESUME_DATA_REQ_MAXIMUM, 10000)]

    resume_data_df = pd.concat(resume_data_list)
    resume_data_df.drop_duplicates(inplace=True)

    # Exclude trace codes for non-agricultural products (aquatic products/livestock products/processed agricultural products).
    resume_data_df = resume_data_df[
        (resume_data_df["TraceCode"].str[:1].astype(int) <= 1)
        & (resume_data_df["OrgID"].str[:1].astype(int) <= 1)
        & (resume_data_df["ParentTraceCode"] == "")
    ]

    resume_data_tbw = resume_data_df.copy()
    resume_data_tbw["sub_product_name"] = resume_data_tbw["ProductName"].str.split("-").str[0]
    resume_data_tbw.rename(columns={
        "TraceCode": "trace_code",
        "ProductName": "product_name",
        "OrgID": "org_id",
        "Producer": "org_name",
        "FarmerName": "farmer_name",
        "PackDate": "pkg_date",
        "StoreInfo": "store_info"
    }, inplace=True)

    resume_data_tbw = resume_data_tbw[[
        "trace_code",
        "product_name",
        "org_id",
        "org_name",
        "farmer_name",
        "pkg_date",
        "store_info",
        "sub_product_name",
        "LandSecNO"
    ]]

    return resume_data_tbw

def process_operation_detail_data(resume_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Process operation detail data extracted from resume data.

    Args:
        resume_data_df (pd.DataFrame): The resume data DataFrame containing the following columns:
            - trace_code
            - LandSecNO

    Returns:
        pd.DataFrame: Processed operation detail data DataFrame containing the following columns:
            - trace_code
            - unit_id: 地政事務所代碼
            - section_id: 段名代碼
            - full_land_no: 完整 8 碼地號
    """
    resume_land_info_tbw = resume_data_df[["trace_code", "LandSecNO"]].copy()
    resume_land_info_tbw["LandSecNO"] = resume_land_info_tbw["LandSecNO"].str.split(";")
    resume_land_info_tbw = resume_land_info_tbw.explode("LandSecNO")
    resume_land_info_tbw["unit_id"] = resume_land_info_tbw["LandSecNO"].str[:2]
    resume_land_info_tbw["section_id"] = resume_land_info_tbw["LandSecNO"].str[2:6]
    resume_land_info_tbw["full_land_no"] = resume_land_info_tbw["LandSecNO"].str[7:15]
    resume_land_info_tbw = resume_land_info_tbw[resume_land_info_tbw["full_land_no"].str.len() == 8]
    resume_land_info_tbw.drop(columns=["LandSecNO"], inplace=True)

    return resume_land_info_tbw
    
def fetch_last_time_update(table_name: str) -> str:
    """
    Fetches the last time the table was updated.
    
    Args:
        db_conn_str (str): The database connection string.
        table_name (str): The table name from which the trace codes need to be fetched.
        
    Returns:
        str: The last time the table was updated.
    """
    
    with create_engine(DB_CONN_STR).connect() as conn_taft:
        try:
            last_time_update_sql = f"SELECT MAX(updated_at) FROM {table_name}"
            last_time_update_df = pd.read_sql(last_time_update_sql, conn_taft)
            last_time_update = last_time_update_df.iloc[0, 0]
            return str(last_time_update)[:10].replace("-", "/")
        except exc.SQLAlchemyError as err_msg:
            print(f"An error occurred while fetching the last time the table was updated: {err_msg}")
            return None
            
    

def main():
    """
    Fetches resume data, processes it, and writes it into the database.

    This function performs the following steps:
    1. Fetches and processes resume data.
    2. Processes operation detail data from the resume data.
    3. Finds records that trace codes not in DB yet and writes the processed resume data and operation detail data into the database tables 'resume_data' and 'resume_land_info'.

    """
    last_time_update = fetch_last_time_update("resume_data")
    resume_data_tbw = fetch_and_process_resume_data(last_time_update)
    resume_land_info_tbw = process_operation_detail_data(resume_data_tbw)
    resume_data_tbw.drop(['LandSecNO'], inplace=True, axis=1)
    print(f'There are {resume_data_tbw.shape[0]} resume data records and {resume_land_info_tbw.shape[0]} resume land info records collected in total.')

    conn_taft = create_engine(DB_CONN_STR)
    with Session(conn_taft) as session:
        try:
            # Get existing trace codes from database
            print('Data fetching and post-processing is done! Now is going to retrieve existed trace code in DB ...')
            resume_data_tc_sql = "SELECT DISTINCT trace_code FROM resume_data"
            resume_land_info_tc_sql = "SELECT DISTINCT trace_code FROM resume_land_info"
            resume_data_existed_tc = pd.read_sql(resume_data_tc_sql, conn_taft)
            print(f'There are {resume_data_existed_tc.shape[0]} unique trace code records in resume_data')
            resume_land_info_existed_tc = pd.read_sql(resume_land_info_tc_sql, conn_taft)
            print(f'There are {resume_land_info_existed_tc.shape[0]} unique trace code records in resume_land_info')
            
            # Compare trace code and write data to the database
            resume_data_tbw_filtered = resume_data_tbw[~resume_data_tbw['trace_code'].isin(resume_data_existed_tc['trace_code'])]
            print(f'There are {resume_data_tbw_filtered.shape[0]} new trace code records is ready to write into table resume_data.')
            resume_data_tbw_filtered.to_sql("resume_data", conn_taft, if_exists="append", index=False)

            resume_land_info_tbw_filtered = resume_land_info_tbw[~resume_land_info_tbw['trace_code'].isin(resume_land_info_existed_tc['trace_code'])]
            print(f'There are {resume_land_info_tbw_filtered.shape[0]} new trace code records is ready to write into table resume_land_info.')
            resume_land_info_tbw_filtered.to_sql("resume_land_info", conn_taft, if_exists="append", index=False)
            session.commit()
            print('DB tables update is done!')
        except exc.SQLAlchemyError as err_msg:
            print(f"An error occurred while writing data into DB: {err_msg}")
            # Perform rollback
            session.rollback()

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"An error occurred: {e}")
