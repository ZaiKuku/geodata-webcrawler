import urllib.parse
import requests
import pandas as pd
from sqlalchemy import create_engine, exc

# Define Variables
OPERATION_DETAIL_API_ENDPOINT = (
    "https://data.coa.gov.tw/Service/OpenData/Resume/OperationDetail_Plus.aspx?"
)

DB_US = "datayoo"
DB_PW = urllib.parse.quote_plus("*@(!)@&#")
DB_HT = "192.168.1.103"
DB_PORT = "3306"
DB_NAME = "taft"
DB_CONN_STR = f"mysql+pymysql://{DB_US}:{DB_PW}@{DB_HT}:{DB_PORT}/{DB_NAME}"

def fetch_operation_detail(trace_code: str) -> pd.DataFrame:
    """
    Fetch operation detail data from API based on the given trace code.

    Args:
        trace_code (str): The trace code for which operation detail data needs to be fetched.

    Returns:
        pd.DataFrame: A DataFrame containing the fetched operation detail data containing the following columns:
            - OperationDate
            - OperationType
            - Operation
            - OperationMemo
            - trace_code
    """
    params = {"Tracecode": trace_code}
    response = requests.get(OPERATION_DETAIL_API_ENDPOINT, params=params)
    response_content = response.json()
    response_df = pd.json_normalize(response_content)
    response_df[response_df == ''] = None
    response_df['trace_code'] = trace_code
    return response_df

def process_and_insert_operation_detail(fetched_operation_detail_df: pd.DataFrame) -> None:
    """
    Process and insert operation detail data into the database.

    Args:
        fetched_operation_detail_df (pd.DataFrame): The resume data DataFrame containing the following columns:
            - OperationDate
            - OperationType
            - Operation
            - OperationMemo
            - trace_code
    """
    fetched_operation_detail_df.rename(columns={
        "OperationDate": "operation_date",
        "OperationType": "operation_type",
        "Operation": "operation_conetent",
        "OperationMemo": "operation_memo"
    }, inplace=True)

    with create_engine(DB_CONN_STR).connect() as conn_taft:
        try:
            print(f"Updating {fetched_operation_detail_df.shape[0]} operation detail info records of trace code: {fetched_operation_detail_df['trace_code'].iloc[0]}")
            fetched_operation_detail_df.to_sql("resume_operation_detail_info", conn_taft, if_exists="append", index=False)
            print("Updating operation is done.")
            conn_taft.commit()
        except exc.SQLAlchemyError as update_err_msg:
            print(f"An error occurred while writing data into DB: {update_err_msg}")
            conn_taft.rollback()

def main() -> None:
    """
    Fetches resume data, processes it, and writes it into the database.

    This function performs the following steps:
    1. Fetches trace codes that missing operation detail information, but already in resume data from the database.
    2. For each missing trace code, fetches operation detail data from the API.
    3. Processes and inserts the fetched operation detail data into the 'resume_operation_detail_info' table in the database.

    """
    with create_engine(DB_CONN_STR).connect() as conn_taft:
        try:
            trace_codes_missing_oper_detail = pd.read_sql(
                '''
                SELECT DISTINCT resume_data.trace_code 
                FROM resume_data 
                LEFT JOIN resume_operation_detail_info 
                ON resume_data.trace_code = resume_operation_detail_info.trace_code 
                WHERE resume_operation_detail_info.operation_type IS NULL;
                ''',
                conn_taft
            )
            print(f"There are {trace_codes_missing_oper_detail.shape[0]} trace code records is missing operation detail info ...")

        except exc.SQLAlchemyError as req_err_msg:
            print(f"An error occurred while writing data into DB: {req_err_msg}")
            conn_taft.rollback()
    
    total_update_records = 0
    for trace_code_i in trace_codes_missing_oper_detail['trace_code']:
        operation_detail_df = fetch_operation_detail(trace_code_i)
        if operation_detail_df.shape[0] == 0:
            continue
        process_and_insert_operation_detail(operation_detail_df)
        total_update_records += operation_detail_df.shape[0]
    print(f"All update is done! There are {total_update_records} record(s) inserted in total.")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"An error occurred: {e}")
