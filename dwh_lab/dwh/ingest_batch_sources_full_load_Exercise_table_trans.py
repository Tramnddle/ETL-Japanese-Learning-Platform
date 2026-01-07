import os
import pandas as pd
from io import StringIO
from io import BytesIO
from datetime import datetime
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
from azure.identity import DefaultAzureCredential
import smtplib
from email.message import EmailMessage
from pathlib import Path
from email.mime.base import MIMEBase
from email import encoders
import clickhouse_connect
import uuid
import argparse

import general_lib
from quality_report import QualityReport
import json
ACCOUNT_NAME = "13f45cadls"
AZURE_DATALAKE_STORAGE_KEY =  "+oB+LkoL2KPaMvZbChL9vKVr/3lFJyDjmHI2cpyFJFDlMFW2pEzPN1zAQbmx9ovFE0hX1vvfll66+ASthCJINQ=="
CLICK_HOUSE_HOST = "kg766z2yms.us-west-2.aws.clickhouse.cloud"
CLICK_HOUSE_USER = "default"
CLICK_HOUSE_PASSWORD = "bhL~qK1keLHlb"

# Source from  
# Get all directory from batch_sources
# Get all entities from each directory --> Full Load
# WRITE CSV, JSON, DELTA to daily path yyyyMMdd_entityname.format
# WRITE TO 01-land-zone/batch_sources/directory
# ARCHIVE 00_FS/batch_sources/archives/junyi
# FAIL --> MOVE TO LOG
# DATA QUALITY CHECK 

SOURCE_CONTAINER = "00fs"
DESTINATION_CONTAINER = "01landzone"
SOURCE_BASE_PATH =  "batch-sources/junyi"

# GET ENTITIES FROM batch_sources

service_client = general_lib.get_azure_service_client_by_account_key(ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY)
file_system_client = service_client.get_file_system_client(file_system= SOURCE_CONTAINER)

# paths = file_system_client.get_paths(path='/{}'.format(SOURCE_BASE_PATH))
# paths = [path.name for path in paths]


def main(entity_path):
    print("Processing: {}".format(entity_path))

    current_date = datetime.now().strftime('%Y%m%d')
    entity_name = entity_path.split("/")[-1].split(".")[0]
    # general_lib.read_chunk_and_writle_dls(SOURCE_CONTAINER, entity_path, DESTINATION_CONTAINER, 
    #         "{}/{}/{}/{}/{}_{}.{}".format(SOURCE_BASE_PATH, entity_name,"json", current_date, current_date, entity_name, "json"),
    #         ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY, "archives/{}/{}/{}/{}/{}_{}.{}".format(SOURCE_BASE_PATH, entity_name,"parquet",current_date, current_date, entity_name, "parquet"))
    
    # # EXTRACT
    df = general_lib.read_azure_datalake_storage(SOURCE_CONTAINER, entity_path, ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY)
    if 'junyi_Exercise_table_trans' in entity_name:
        df = df.sample(frac=0.1, random_state=42)

    # # LOAD
    current_date = datetime.now().strftime('%Y%m%d')
    entity_name = entity_path.split("/")[-1].split(".")[0]
    if 'junyi_Exercise_table_trans' in entity_name:
        entity_name = 'junyi_Exercise_table_trans'
    general_lib.write_dls(df, "jsonline", ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY, 
                            DESTINATION_CONTAINER, 
                            "{}/{}/{}/{}/{}_{}.{}".format(SOURCE_BASE_PATH, entity_name,"json", current_date, current_date, entity_name, "json")
                        )

    # ARCHIVE
    general_lib.write_dls(df, "parquet", ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY, 
                            SOURCE_CONTAINER, 
                            "archives/{}/{}/{}/{}/{}_{}.{}".format(SOURCE_BASE_PATH, entity_name,"parquet",current_date, current_date, entity_name, "parquet")
                        )
    
    qr = QualityReport(df, entity_name= "ingest batch full")
    qr.summarize_volume()
    qr.check_nulls(required_columns=['name', 'live', 'pretty_display_name', 'short_display_name',
        'topic', 'area', 'creation_date', 'seconds_per_fast_problem'])  # hoặc cột tùy theo entity
    qr.check_duplicates(dedup_columns=['name'])  # hoặc các cột phù hợp
    qr.check_array_fields(array_fields=['prerequisites'])
    # qr.compare_with_source(df, key_cols=["id", "question_id"])

    report = qr.generate()
    # ✅ Flatten the JSON to 1 row with dot-notation keys
    report = pd.json_normalize(report).to_dict(orient="records")[0]
    # 👉 Add source volume info
    row_count = len(df)
    report["source_volume"] = row_count

    JOB_DATE = datetime.now().strftime('%Y%m%d')
    os.makedirs("reports", exist_ok=True)
    with open(f"reports/quality_report_ingest_batch_Exercise_table_trans/quality_report_ingest_batch_full{entity_name}_{JOB_DATE}.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)

    print(f"✅ Quality report saved to: reports/quality_report_ingest_batch_full{entity_name}_{JOB_DATE}.json")
        
if __name__ == '__main__':
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Ingest Batch Sources")
    parser.add_argument('--entity_path', type=str, required=True, help="entity_path to ingest")
    
    # Parse the arguments
    args = parser.parse_args()
    main(args.entity_path)
        
