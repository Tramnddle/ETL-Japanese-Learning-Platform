import general_lib
import argparse
from datetime import datetime
from quality_report import QualityReport
import json, os
import pandas as pd



ACCOUNT_NAME = "13f45cadls"
AZURE_DATALAKE_STORAGE_KEY =  "+oB+LkoL2KPaMvZbChL9vKVr/3lFJyDjmHI2cpyFJFDlMFW2pEzPN1zAQbmx9ovFE0hX1vvfll66+ASthCJINQ=="
CLICK_HOUSE_HOST = "kg766z2yms.us-west-2.aws.clickhouse.cloud"
CLICK_HOUSE_USER = "default"
CLICK_HOUSE_PASSWORD = "bhL~qK1keLHlb"
SOURCE_CONTAINER = "00fs"
DESTINATION_CONTAINER = "01landzone"
BASE_PATH = "streaming-sources"


def main(entity_name):
    click_house_client =  general_lib.get_click_house_client( CLICK_HOUSE_HOST, CLICK_HOUSE_USER, CLICK_HOUSE_PASSWORD)

    # full_load_table_list = [
    #     "options",
    #     "questions"
    # ]
    

    df = general_lib.read_click_house(click_house_client, "SELECT * FROM {}".format(entity_name))
    
    # LOAD
    current_date = datetime.now().strftime('%Y%m%d')
    general_lib.write_dls(df, "jsonline", ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY, 
                            DESTINATION_CONTAINER, 
                            "{}/{}/{}/{}/{}_{}.{}".format(BASE_PATH, entity_name,"json", current_date, current_date, entity_name, "json")
                        )

    # ARCHIVE
    df = df.applymap(general_lib.convert_uuid)
    general_lib.write_dls(df, "parquet", ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY, 
        SOURCE_CONTAINER, 
    "archives/{}/{}/{}/{}/{}_{}.{}".format(BASE_PATH, entity_name,"parquet",current_date, current_date, entity_name, "parquet")
)
    
        # QUALITY REPORT
    qr = QualityReport(df, entity_name="click_house_full")
    qr.summarize_volume()
    qr.check_nulls(required_columns=["id", "question_id", "option_text","question_text", "type","created_at"])
    qr.check_duplicates(dedup_columns=['id', "question_id"])
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

    report = qr.generate()
    report = pd.json_normalize(report).to_dict(orient="records")[0]

    JOB_DATE = datetime.now().strftime('%Y%m%d')
    os.makedirs("reports", exist_ok=True)
    with open(f"reports/quality_report_ingest_fullload_{entity_name}/quality_report_ingest_fullload_{entity_name}_{JOB_DATE}.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)

    print(f"✅ Quality report saved to: reports/quality_report_ingest_fullload_{entity_name}_{JOB_DATE}.json")
if __name__ == '__main__':
    # Set up argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('--entity_name', type=str, required=True)
    
    # Parse the arguments
    args = parser.parse_args()
    main(args.entity_name)
        
