import urllib.parse
import requests
import pandas as pd
from sqlalchemy import create_engine

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
def get_resume_data(skip):
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

# Loop through skip values and download resume data
resume_data_list = [get_resume_data(skip_i) for skip_i in range(0, RESUME_DATA_REQ_MAXIMUM, 10000)]

### Concatenate all resume data
resume_data_df = pd.concat(resume_data_list)

### Prepare resume data
# 剔除非農產品 (水產品/畜產品/加工農產品) 的 trace code
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
    "sub_product_name"
]]

### Prepare resume land info data
resume_land_info_tbw = resume_data_df[["TraceCode", "LandSecNO"]].copy()
resume_land_info_tbw["LandSecNO"] = resume_land_info_tbw["LandSecNO"].str.split(";")
resume_land_info_tbw = resume_land_info_tbw.explode("LandSecNO")
resume_land_info_tbw["unit_id"] = resume_land_info_tbw["LandSecNO"].str[:2]
resume_land_info_tbw["section_id"] = resume_land_info_tbw["LandSecNO"].str[2:6]
resume_land_info_tbw["full_land_no"] = resume_land_info_tbw["LandSecNO"].str[7:15]
resume_land_info_tbw = resume_land_info_tbw[resume_land_info_tbw["full_land_no"].str.len() == 8]
resume_land_info_tbw.rename(columns={"TraceCode": "trace_code"}, inplace=True)
resume_land_info_tbw.drop(columns=["LandSecNO"], inplace=True)

##### DB write-in
# Database connection
with create_engine(DB_CONN_STR).connect() as conn_taft:
    try:
        # Get existing trace codes from database
        resume_data_tc_sql = "SELECT DISTINCT trace_code FROM resume_data"
        resume_land_info_tc_sql = "SELECT DISTINCT trace_code FROM resume_land_info"
        resume_data_existed_tc = pd.read_sql(resume_data_tc_sql, conn_taft)
        resume_land_info_existed_tc = pd.read_sql(resume_land_info_tc_sql, conn_taft)

        # Write data to the database
        resume_data_tbw_filtered = resume_data_tbw[~resume_data_tbw['trace_code'].isin(resume_data_existed_tc['trace_code'])]
        resume_data_tbw_filtered.to_sql("resume_data", conn_taft, if_exists="append", index=False)

        resume_land_info_tbw_filtered = resume_land_info_tbw[~resume_land_info_tbw['trace_code'].isin(resume_data_existed_tc['trace_code'])]
        resume_land_info_tbw_filtered.to_sql("resume_land_info", conn_taft, if_exists="append", index=False)

    except exc.SQLAlchemyError as e:
        # Handle the exception
        print(f"An error occurred while writing data into DB: {e}")
        # Perform rollback
        conn_taft.rollback()
