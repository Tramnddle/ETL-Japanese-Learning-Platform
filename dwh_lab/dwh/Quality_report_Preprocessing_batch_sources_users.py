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
DESTINATION_CONTAINER = "02bronze"
JOB_DATE = "20250604"
SOURCE_BASE_PATH = "batch-sources/junyi/junyi_ProblemLog_original/json"
DESTINATION_BASE_PATH = "batch-sources/junyi/preprocessing_batch_sources_users/json"
file_path_destination = f"{DESTINATION_BASE_PATH}/{JOB_DATE}/{JOB_DATE}_junyi_users_process.json"
file_path_source = f"{SOURCE_BASE_PATH}/{JOB_DATE}/{JOB_DATE}_junyi_ProblemLog_original.json"

# === Load Transformed Data ===
transformed_df = general_lib.read_azure_datalake_storage(DESTINATION_CONTAINER, file_path_destination, ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY)
if transformed_df.empty:
    print("❌ No data loaded from transformed file.")
    exit()

# === Run Quality Report ===
qr = QualityReport(transformed_df, entity_name="batch_user")

qr.summarize_volume()
qr.check_nulls(required_columns=[
    'user_id', 'username', 'password', 'created_time', 'preferred_content_type'
])
qr.check_formats(expected_dtypes={
    'user_id': 'Int64',
    'username': 'object',
    "password": 'object',
    'created_time': 'datetime64[ns]',
    'preferred_content_type': 'object'
})
qr.check_default_values(default_values={
    'source_id': 2,
    'is_update': 0,
    'is_delete': 0
})
qr.check_duplicates(dedup_columns=['user_id', 'username', 'password'])
qr.check_array_fields(array_fields=[
    'preferred_content_type', 'preferred_learn_styles',
    'education_lv', 'preferred_areas'
])

# === Track Clean Success Rates ===
qr.track_clean_success_rate('email', lambda x: bool(re.match(r"[^@]+@[^@]+\.[^@]+", str(x))))
qr.track_clean_success_rate('preferred_content_types', lambda x: isinstance(x, list) and len(x) > 0)
qr.track_clean_success_rate('preferred_learn_styles', lambda x: isinstance(x, list) and len(x) > 0)
qr.track_clean_success_rate('preferred_areas', lambda x: isinstance(x, list) and len(x) > 0)
qr.track_clean_success_rate('education_lv', lambda x: str(x).lower() in {"none of the above"})

# === Optional: Compare with source ===
source_df = general_lib.read_azure_datalake_storage(SOURCE_CONTAINER, file_path_source, ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY)

qr.compare_with_source(source_df, key_cols=source_df.columns)
# === Save Report ===
report = qr.generate()
report = pd.json_normalize(report).to_dict(orient="records")[0]

os.makedirs("reports", exist_ok=True)
report_path = f"reports/quality_report_batch_users/quality_report_batch_users_{JOB_DATE}.json"
with open(report_path, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2, ensure_ascii=False, default=str)

print(f"✅ Quality report saved to: {report_path}")
