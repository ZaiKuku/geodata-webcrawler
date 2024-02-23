import urllib.parse
import requests
import pandas as pd
from sqlalchemy import create_engine, exc

# Define Variables
API_ENDPOINT = "https://data.coa.gov.tw/Service/OpenData/Resume/ResumeData_Plus.aspx"
RESUME_DATA_REQ_MAXIMUM = 20000

DB_US = "datayoo"
DB_PW = urllib.parse.quote_plus("*@(!)@&#")
DB_HT = "192.168.1.103"
DB_PORT = "3306"
DB_NAME = "taft"
DB_CONN_STR = f"mysql+pymysql://{DB_US}:{DB_PW}@{DB_HT}:{DB_PORT}/{DB_NAME}"

##### resume data retrieve
### request exterior api
# Define the parameters
params = {"$top": 10000, "$skip": 0}

# Define the function to download and save resume data
def get_resume_data(skip: int) -> pd.DataFrame:
    """
    Download and save resume data.

    Args:
        skip (int): The number of records to skip.

    Returns:
        pandas.DataFrame: The resume data as a DataFrame.
    """
    print("collecting taft resume data with params skip = " + str(skip))
    params["$skip"] = skip
    response = requests.get(API_ENDPOINT, params=params)
    data = response.json()
    df = pd.json_normalize(data)
    print("Data is retrieved successfully! There are " + str(df.shape[0]) + " row(s) returned in total.")
    df.rename(columns={"Tracecode": "TraceCode"}, inplace=True)
    return df

def fetch_and_process_resume_data() -> pd.DataFrame:
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
    resume_data_list = [get_resume_data(skip_i) for skip_i in range(0, RESUME_DATA_REQ_MAXIMUM, 10000)]

    ### Concatenate all resume data
    resume_data_df = pd.concat(resume_data_list)

    ### Prepare resume data
    # Exclude trace codes for non-agricultural products (aquatic products/livestock products/processed agricultural products).
    resume_data_df = resume_data_df[
        (resume_data_df["TraceCode"].str[:1].astype(int) <= 1)
        & (resume_data_df["OrgID"].str[:1].astype(int) <= 1)
        & (resume_data_df["ParentTraceCode"] == "")
    ]

    resume_data_tbw = resume_data_df.copy()
    resume_data_tbw["sub_product_name"] = resume_data_tbw["ProductName"].str.split("-").str[0]
    resume_data_processed_full = resume_data_tbw.rename(columns={
        "TraceCode": "trace_code",
        "ProductName": "product_name",
        "OrgID": "org_id",
        "Producer": "org_name",
        "FarmerName": "farmer_name",
        "PackDate": "pkg_date",
        "StoreInfo": "store_info"
    }, inplace=False)

    resume_data_tbw = resume_data_processed_full[[
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

def main():
    """
    Fetches resume data, processes it, and writes it into the database.

    This function performs the following steps:
    1. Fetches and processes resume data.
    2. Processes operation detail data from the resume data.
    3. Finds records that trace codes not in DB yet and writes the processed resume data and operation detail data into the database tables 'resume_data' and 'resume_land_info'.

    """
    resume_data_tbw = fetch_and_process_resume_data()
    resume_land_info_tbw = process_operation_detail_data(resume_data_tbw)
    resume_data_tbw.drop(['LandSecNO'], inplace=True, axis=1)
    print(f'There are {resume_data_tbw.shape[0]} resume data records and {resume_land_info_tbw.shape[0]} resume land info records collected in total.')

    with create_engine(DB_CONN_STR).connect() as conn_taft:
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
            conn_taft.commit()
            print('DB tables update is done!')

        except exc.SQLAlchemyError as err_msg:
            # Handle the exception
            print(f"An error occurred while writing data into DB: {err_msg}")
            # Perform rollback
            conn_taft.rollback()

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"An error occurred: {e}")
