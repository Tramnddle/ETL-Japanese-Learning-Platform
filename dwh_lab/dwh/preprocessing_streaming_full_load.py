import general_lib
import argparse
from datetime import datetime
import pandas as pd
from quality_report import QualityReport
import os, json

ACCOUNT_NAME = "13f45cadls"
AZURE_DATALAKE_STORAGE_KEY =  "+oB+LkoL2KPaMvZbChL9vKVr/3lFJyDjmHI2cpyFJFDlMFW2pEzPN1zAQbmx9ovFE0hX1vvfll66+ASthCJINQ=="
CLICK_HOUSE_HOST = "kg766z2yms.us-west-2.aws.clickhouse.cloud"
CLICK_HOUSE_USER = "default"
CLICK_HOUSE_PASSWORD = "bhL~qK1keLHlb"
SOURCE_CONTAINER = "01landzone"
DESTINATION_CONTAINER = "02bronze"
BASE_PATH = "streaming-sources"

def add_additional_columns(df: pd.DataFrame) -> pd.DataFrame:
    current_time = datetime.now()
    df = df.copy()  # avoid modifying the original DataFrame

    df['source_name'] = 'clickhouse-streaming-data'
    df['source_id'] = 1
    df['is_update'] = False
    df['is_delete'] = False
    df['created_time'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
    df['created_date'] = current_time.date().strftime('%Y-%m-%d')

    return df


def main(entity_name):
    
    current_date = datetime.now().strftime('%Y%m%d')
    # READ DATA FROM
    entity_path = f"{BASE_PATH}/{entity_name}/json/{current_date}/{current_date}_{entity_name}.json"
    source_df = general_lib.read_azure_datalake_storage(SOURCE_CONTAINER, entity_path, ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY)
    transformed_df = add_additional_columns(source_df)

    # LOAD
    general_lib.write_dls(transformed_df, "json", ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY, 
                            DESTINATION_CONTAINER, 
                            "{}/{}/{}/{}/{}_{}.{}".format(BASE_PATH, entity_name,"json", current_date, current_date, entity_name, "json")
                        )
    
    qr = QualityReport(transformed_df, entity_name="streaming_fullload")
    qr.summarize_volume()
    qr.check_nulls(required_columns=["id", "question_id", "option_text","question_text", "type","created_at"])
    qr.check_duplicates(dedup_columns=['user_id'])
    qr.check_formats(expected_dtypes={
    "id": "string",
    "question_id": "string",
    "question_text": "string",
    "option_text": "string",
    "type": "string",
    "created_at": "datetime64[ns]",
    "form": "string",
    "created_time": "datetime64[ns]",
    "created_date": "object"
    })
    
    qr.compare_with_source(source_df, key_cols=["id", "question_id","option_id", "option_text","question_text", "type","created_at", "form"])
    report = qr.generate()
    report = pd.json_normalize(report).to_dict(orient="records")[0]
    JOB_DATE = datetime.now().strftime('%Y%m%d')
    os.makedirs("reports", exist_ok=True)
    with open(f"reports/quality_report_preprocessing_{entity_name}/quality_report_{entity_name}_{JOB_DATE}.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)

    print(f"✅ Quality report saved to: reports/quality_report_{entity_name}_{JOB_DATE}.json")
   
        
if __name__ == '__main__':
    # Set up argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('--entity_name', type=str, required=True)
    
    # Parse the arguments
    args = parser.parse_args()
    main(args.entity_name)
        
