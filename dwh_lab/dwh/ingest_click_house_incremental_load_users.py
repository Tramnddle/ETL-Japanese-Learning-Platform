import general_lib
import argparse
from datetime import datetime
from quality_report import QualityReport
import json
import os
import pandas as pd


ACCOUNT_NAME = "13f45cadls"
AZURE_DATALAKE_STORAGE_KEY =  "+oB+LkoL2KPaMvZbChL9vKVr/3lFJyDjmHI2cpyFJFDlMFW2pEzPN1zAQbmx9ovFE0hX1vvfll66+ASthCJINQ=="
CLICK_HOUSE_HOST = "kg766z2yms.us-west-2.aws.clickhouse.cloud"
CLICK_HOUSE_USER = "default"
CLICK_HOUSE_PASSWORD = "bhL~qK1keLHlb"
SOURCE_CONTAINER = "00fs"
DESTINATION_CONTAINER = "01landzone"
BASE_PATH = "streaming-sources"



WATERMARK_CONTAINER =  "00fs"
WATERMARK_PATH =  "watermark_table.csv"
BASE_PATH = "streaming-sources"

def main(entity_name):
    click_house_client =  general_lib.get_click_house_client( CLICK_HOUSE_HOST, CLICK_HOUSE_USER, CLICK_HOUSE_PASSWORD)
    # READ WATERMARK
    watermark_df = general_lib.read_azure_datalake_storage(WATERMARK_CONTAINER, WATERMARK_PATH, ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY)
    df_filtered = watermark_df[watermark_df['table_name'] == entity_name]
    watermark_value = df_filtered['watermark_value'].iloc[0] if not df_filtered.empty else None
    update_column = df_filtered['update_column'].iloc[0] if not df_filtered.empty else None
    
    # EXTRACT
    df = general_lib.read_click_house(
        click_house_client, "SELECT * FROM {} where {} > '{}'".format(entity_name, update_column, watermark_value))

    # UPDATE WATERMARK VALUE
    if not df.empty:
        new_watermark_value = df[update_column].max()
        watermark_df.loc[watermark_df['table_name'] == entity_name, 'watermark_value'] = new_watermark_value
        general_lib.write_dls(watermark_df, "csv", ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY, 
                            WATERMARK_CONTAINER, 
                            WATERMARK_PATH)
        print(f"Watermark updated to {new_watermark_value}")
    else:
        print("No data extracted, watermark not updated.")
        
    
    # LOAD
    current_date = datetime.now().strftime('%Y%m%d')
    current_datetime = datetime.now().strftime('%Y%m%d%H%M%S')
    
    general_lib.write_dls(df, "json", ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY, 
                    DESTINATION_CONTAINER, 
                    "{}/{}/{}/{}/{}_{}.{}".format(BASE_PATH, entity_name,"json", current_date, current_datetime, entity_name, "json")
                )

    # ARCHIVE
    df = df.applymap(general_lib.convert_uuid)
    general_lib.write_dls(df, "parquet", ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY, 
        SOURCE_CONTAINER, 
    "archives/{}/{}/{}/{}/{}_{}.{}".format(BASE_PATH, entity_name,"parquet", current_date, current_datetime, entity_name, "parquet")
   )
    
    # ✅ QUALITY REPORT
    qr = QualityReport(df, entity_name="users")
    qr.summarize_volume()
    qr.check_nulls(required_columns=['user_id', 'username','email','preferred_areas','preferred_content_types','preferred_learn_style','education_lv'])
    qr.check_duplicates(dedup_columns=['user_id','username','email','preferred_areas','preferred_content_types','preferred_learn_style','education_lv'])

    report = pd.json_normalize(qr.generate()).to_dict(orient="records")[0]

    JOB_DATE = datetime.now().strftime('%Y%m%d')
    os.makedirs("reports", exist_ok=True)
    with open(f"reports/quality_report_clickhouse_users/quality_report_clickhouse_incremental_{entity_name}_{JOB_DATE}.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)

    print(f"✅ Quality report saved to: reports/quality_report_clickhouse_incremental_{entity_name}_{JOB_DATE}.json")

   
if __name__ == '__main__':
    # Set up argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('--entity_name', type=str, required=True)
    
    # Parse the arguments
    args = parser.parse_args()
    main(args.entity_name)
