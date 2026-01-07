import os
import re
import json
import pandas as pd
from datetime import datetime
from quality_report import QualityReport
import general_lib

# === Configuration ===
ACCOUNT_NAME = "13f45cadls"
AZURE_DATALAKE_STORAGE_KEY = "+oB+LkoL2KPaMvZbChL9vKVr/3lFJyDjmHI2cpyFJFDlMFW2pEzPN1zAQbmx9ovFE0hX1vvfll66+ASthCJINQ=="
SOURCE_CONTAINER = "01landzone"
JOB_DATE = "20250604"
PROBLEM_SOURCE_FILE_BASE_PATH = "batch-sources/junyi/junyi_ProblemLog_original/json"
EXCERCISE_SOURCE_FILE_BASE_PATH = "batch-sources/junyi/junyi_Exercise_table_trans/json"
DESTINATION_BASE_PATH = "batch-sources/junyi/preprocessing_batch_sources_browsing_history/json"

# === Load Source File ===
file_path_problem = f"{PROBLEM_SOURCE_FILE_BASE_PATH}/{JOB_DATE}/{JOB_DATE}_junyi_ProblemLog_original.json"
df_problem = general_lib.read_azure_datalake_storage(SOURCE_CONTAINER, file_path_problem, ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY)
file_path_excercise = f"{EXCERCISE_SOURCE_FILE_BASE_PATH}/{JOB_DATE}/{JOB_DATE}_junyi_Exercise_table_trans.json"
df_excercise = general_lib.read_azure_datalake_storage(SOURCE_CONTAINER, file_path_excercise, ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY)
# === Load Transformed Data ===
file_path_transformed = f"{DESTINATION_BASE_PATH}/{JOB_DATE}/{JOB_DATE}_junyi_browsing_process.json"
df_transformed = general_lib.read_azure_datalake_storage("02bronze", file_path_transformed, ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY)

# === Run Quality Report ===
qr = QualityReport(df_transformed, entity_name="browsing_history")

qr.summarize_volume()
qr.check_nulls(required_columns=[
    'user_id', 'username', 'password', 'created_time', 'preferred_content_type',
    'entry_id', 'pageview_count', 'referrer_page', 'search_keyword', 'title', 'url', 'tmp_keywords', 'timestamp'
])

qr.check_formats(expected_dtypes={
    'entry_id': 'object',
    'timestamp': 'datetime64[ns]',
    'pageview_count': 'Int64',
    'search_keyword': 'object',
    'tmp_keywords': 'object'
}) # thêm các trường còn lại trong file, bên trên thiếu
qr.check_default_values(default_values={
    'source_id': 2,
    'is_update': "FALSE",
    'is_delete': "FALSE"
}) #tự thêm theo ý các trường còn lại
qr.check_duplicates(dedup_columns=['entry_id'])
qr.check_array_fields(array_fields=['search_keyword', 'tmp_keywords'])

# === Track Clean Success Rates ===
qr.track_clean_success_rate("title", lambda x: isinstance(x, str) and x.strip() != "")
qr.track_clean_success_rate("url", lambda x: isinstance(x, str) and x.startswith("http"))
qr.track_clean_success_rate("timestamp", lambda x: pd.notnull(x) and isinstance(x, pd.Timestamp))
qr.track_clean_success_rate("pageview_count", lambda x: pd.notnull(x) and isinstance(x, (int, float)) and x >= 0)
qr.track_clean_success_rate("referrer_page", lambda x: isinstance(x, str))
qr.track_clean_success_rate("tmp_keywords", lambda x: isinstance(x, list) and len(x) > 0)
qr.track_clean_success_rate("visible_content", lambda x: isinstance(x, str) and len(x.strip()) > 0)

# === Compare with Source ===
qr.compare_with_source(df_problem, key_cols=df_problem.columns)

qr.compare_with_source(df_excercise, key_cols=df_excercise.columns)

# === Save Report ===
report = qr.generate()
report = pd.json_normalize(report).to_dict(orient="records")[0]

os.makedirs("reports", exist_ok=True)
report_path = f"reports/quality_report_batch_browsing/quality_report_batch_browsing_{JOB_DATE}.json"
with open(report_path, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2, ensure_ascii=False, default=str)

print(f"✅ Quality report saved to: {report_path}")
