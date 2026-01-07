import general_lib
import argparse
from datetime import datetime
# from prettytable import PrettyTable
import nltk
from nltk.util import ngrams
from nltk.corpus import stopwords
from dateutil import parser
import re
# import spacy
import json
import uuid
from collections import Counter
import pandas as pd
from datetime import timedelta
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

nltk.download('stopwords')
nltk.download('punkt')
nltk.download('punkt_tab')

WATERMARK_CONTAINER =  "01landzone"
WATERMARK_PATH =  "watermark_table.csv"
BASE_PATH = "streaming-sources"


def incremental_load(entity_name):
    print("===== Processing "+entity_name)
    parent_path = f"{BASE_PATH}/{entity_name}/json"
    folders = general_lib.list_folder_path(SOURCE_CONTAINER, ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY, parent_path)
    # READ WATERMARK
    watermark_df = general_lib.read_azure_datalake_storage(WATERMARK_CONTAINER, WATERMARK_PATH, ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY)
    df_filtered = watermark_df[watermark_df['table_name'] == entity_name]
    watermark_value = df_filtered['watermark_value'].iloc[0] if not df_filtered.empty else None
    # update_column = df_filtered['update_column'].iloc[0] if not df_filtered.empty else None
    
    # EXTRACT
    # FIND DATE > WATERMARK
    watermark_dt = datetime.strptime(str(watermark_value), '%Y%m%d%H%M%S') if watermark_value else datetime.min
    watermark_date = watermark_dt.strftime('%Y%m%d')
    # Filter folders
      # 🟡 NEW: Filter folders > watermark_date
    folders_to_extract = [folder for folder in folders if folder > watermark_date]
    print("Folders to extract:")
    print(folders_to_extract)

    files_to_extract = []
    for folder in folders_to_extract:
        data_path = f"{BASE_PATH}/{entity_name}/json/{folder}"
        files = general_lib.list_file_path(SOURCE_CONTAINER, ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY, data_path)

        for file_path in files:
            filename = file_path.split('/')[-1]  # e.g., '20250508033853_users.json'
            timestamp_str = filename.split('_')[0]  # e.g., '20250508033853'
            try:
                file_dt = datetime.strptime(timestamp_str, '%Y%m%d%H%M%S')
                if file_dt > watermark_dt:
                    files_to_extract.append((file_dt, file_path))
            except ValueError:
                continue  # Skip invalid formats

    files_to_extract.sort()
    print("Files to extract:")
    for _, f in files_to_extract:
        print(f)

    dfs = []
    for _, file_path in files_to_extract:
        df = general_lib.read_azure_datalake_storage(SOURCE_CONTAINER, file_path, ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY)
        if df.empty:
            continue
        dfs.append(df)

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        print(f"Combined total {len(combined_df)} records from all data")

        # 🟡 NEW: Update watermark to latest file datetime
        new_watermark_value = max([dt for dt, _ in files_to_extract]).strftime('%Y%m%d%H%M%S')
        watermark_df.loc[watermark_df['table_name'] == entity_name, 'watermark_value'] = new_watermark_value
        general_lib.write_dls(watermark_df, "csv", ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY, 
                              WATERMARK_CONTAINER, 
                              WATERMARK_PATH)
        print(f"Watermark updated to {new_watermark_value}")
    else:
        combined_df = pd.DataFrame()
        print("No data extracted, watermark not updated.")

    return combined_df
   
def remove_duplicates_browsing_history(data):
    unique_records = []
    seen = set()
    for record in data:
        def make_hashable(val):
            return json.dumps(val, sort_keys=True) if isinstance(val, dict) else val

        key = tuple(make_hashable(record[i]) for i in [2, 5, 6, 8, 9, 10])
        # key = (record[2], record[5], record[6], record[8], record[9], record[10])
        print("key info: ", key)
        if key not in seen:
            seen.add(key)
            unique_records.append(record)
    return unique_records


def add_window_time_columns(data):
    if not data:
        return []

    # In ra một mẫu dữ liệu đầu tiên để xem cấu trúc
    first_item = data[0]
    print("First item type:", type(first_item))
    if hasattr(first_item, '_fields'):
        print("Available fields:", first_item._fields)

    # Kiểm tra xem dữ liệu là namedtuple hay không
    result = []

    for item in data:
        # Tạo dictionary từ namedtuple hoặc tuple thông thường
        if hasattr(item, '_asdict'):
            # Nếu là namedtuple, dùng _asdict() để chuyển sang dictionary
            record = item._asdict()
        elif hasattr(item, '_fields'):
            # Cách khác để chuyển namedtuple sang dictionary
            record = {field: getattr(item, field) for field in item._fields}
        else:
            # Nếu không phải namedtuple, giả sử đây là tuple bình thường
            # và Feature_Name('browsinghistory') trả về danh sách cột
            fields = ['entry_id', 'exit_page', 'pageview_count', 'referrer_page',
                      'search_keyword', 'timestamp', 'title', 'tmp_keywords',
                      'url', 'user_id', 'visible_content']
            record = {fields[i]: item[i] for i in range(min(len(fields), len(item)))}

        # Bây giờ thêm các trường window_time
        try:
            if 'timestamp' in record:
                timestamp_str = record['timestamp']
                timestamp = parser.parse(str(timestamp_str))
            else:
                # Nếu không tìm thấy trường timestamp, in thông báo và bỏ qua
                print("No timestamp field found in record:", list(record.keys()))
                continue

        except Exception as e:
            # Sử dụng record.get để tránh KeyError
            print(f"Error parsing timestamp: {record.get('timestamp', 'N/A')} - {e}")
            continue

        hour = timestamp.hour

        # Xác định window_time_details và window_time_details_meaning
        record['window_time_details'] = hour
        record['window_time_details_meaning'] = f"{hour}h{'am' if hour < 12 else 'pm'}"

        # Xác định window_time_overall và window_time_overall_meaning
        if 0 <= hour < 6:
            record['window_time_overall'] = 0
            record['window_time_overall_meaning'] = '(0h - 6h) am'
        elif 6 <= hour < 12:
            record['window_time_overall'] = 6
            record['window_time_overall_meaning'] = '(6h - 12h) am'
        elif 12 <= hour < 18:
            record['window_time_overall'] = 12
            record['window_time_overall_meaning'] = '(12h - 18h) pm'
        else:
            record['window_time_overall'] = 18
            record['window_time_overall_meaning'] = '(18h - 24h) pm'

        result.append(record)

    # Sắp xếp kết quả theo timestamp tăng dần
    return sorted(result, key=lambda x: parser.parse(str(x['timestamp'])))

def clean_text(text):
    if not text or not isinstance(text, str):
        return ""
    return re.sub(r'[^\w\s_-]', '', text).lower()

def extract_keywords_from_visible_content(visible_content, stop_words, n=10):
    if not visible_content or not isinstance(visible_content, str):
        return []

    text = visible_content[:10000] if len(visible_content) > 10000 else visible_content

    cleaned_text = clean_text(text)
    tokens = nltk.word_tokenize(cleaned_text)

    filtered_tokens = [token for token in tokens if token.lower() not in stop_words and len(token) > 1]

    keywords = filtered_tokens.copy()

    bi_grams = list(ngrams(filtered_tokens, 2))
    for gram in bi_grams:
        if all(len(token) > 1 for token in gram):
            keywords.append(' '.join(gram))

    tri_grams = list(ngrams(filtered_tokens, 3))
    for gram in tri_grams:
        if all(len(token) > 1 for token in gram):
            keywords.append(' '.join(gram))

    word_freq = Counter(keywords)

    sorted_keywords = sorted(word_freq.items(),
                           key=lambda x: (x[1], len(x[0])),
                           reverse=True)

    return [kw for kw, _ in sorted_keywords[:n]]

def normalize_keyword(keyword):
    if not keyword:
        return ""
    normalized = keyword.lower().replace('_', ' ').replace('-', ' ')
    return ' '.join(normalized.split())

def add_exact_keywords_column(data):
    if not data:
        return []

    print("Bắt đầu thêm cột exact_keywords...")

    # Chuẩn bị stop words sử dụng NLTK
    stop_words = set(stopwords.words('english'))

    # Bổ sung thêm một số stop words phổ biến
    additional_stop_words = {"a", "an", "the", "this", "that", "these", "those",
                           "i", "you", "he", "she", "it", "we", "they",
                           "am", "is", "are", "was", "were", "been", "be",
                           "have", "has", "had", "do", "does", "did",
                           "will", "would", "shall", "should", "can", "could",
                           "and", "or", "but", "if", "then", "else", "when",
                           "where", "why", "how", "what", "which", "who",
                           "all", "any", "both", "each", "few", "more", "most",
                           "other", "some", "such", "no", "nor", "not", "only",
                           "own", "same", "so", "than", "too", "very",
                           "here", "there", "now", "ever", "never", "also"}
    stop_words.update(additional_stop_words)
    print(f"Đã tải {len(stop_words)} stop words")

    # Xử lý dữ liệu thành dạng dictionary
    # Chuyển tất cả bản ghi thành dictionary, giữ nguyên các trường gốc
    if hasattr(data[0], '_asdict'):
        data_as_dicts = [item._asdict() for item in data]
    elif hasattr(data[0], '_fields'):
        data_as_dicts = [{field: getattr(item, field) for field in item._fields} for item in data]
    else:
        data_as_dicts = [dict(item) if isinstance(item, dict) else item for item in data]


    # Số lượng bản ghi cần xử lý
    total_records = len(data_as_dicts)
    print(f"Đang xử lý {total_records} bản ghi...")

    for i, record in enumerate(data_as_dicts):
        if (i + 1) % 100 == 0 or i + 1 == total_records:
            print(f"Đã xử lý {i+1}/{total_records} bản ghi")

        # Khởi tạo exact_keywords
        exact_keywords = set()

        # i. Thêm từ trường search_keyword
        search_keyword = record.get('search_keyword', '')
        if search_keyword and isinstance(search_keyword, str):
            cleaned_search_keyword = clean_text(search_keyword)
            if cleaned_search_keyword:
                normalized = normalize_keyword(cleaned_search_keyword)
                if normalized and all(word not in stop_words for word in normalized.split()):
                    exact_keywords.add(normalized)

        # i. Thêm từ trường tmp_keywords
        tmp_keywords = record.get('tmp_keywords', '')
        if tmp_keywords:
            if isinstance(tmp_keywords, str):
                tmp_kw_list = tmp_keywords.split(',')
            elif isinstance(tmp_keywords, list):
                tmp_kw_list = tmp_keywords
            else:
                tmp_kw_list = []

            for kw in tmp_kw_list:
                cleaned_kw = clean_text(kw)
                if cleaned_kw:
                    normalized = normalize_keyword(cleaned_kw)
                    if normalized and all(word not in stop_words for word in normalized.split()):
                        exact_keywords.add(normalized)

        # v. Extract tối đa 10 keyword từ visible_content
        visible_content = record.get('visible_content', '')
        content_keywords = extract_keywords_from_visible_content(visible_content, stop_words)

        # vi. Thêm các keyword chưa tồn tại
        for kw in content_keywords:
            normalized = normalize_keyword(kw)
            if normalized and all(word not in stop_words for word in normalized.split()) and normalized not in exact_keywords:
                exact_keywords.add(normalized)

        # Chuyển set thành list
        record['exact_keywords'] = list(exact_keywords)

    print("Hoàn thành thêm cột exact_keywords!")
    return data_as_dicts

def assign_session_ids(data, session_timeout=timedelta(minutes=30)):
    session_id = 1
    previous_timestamp = None

    for record in data:
        try:
            timestamp_str = record['timestamp']
            current_timestamp = parser.parse(str(timestamp_str))
            record['timestamp'] = current_timestamp
        except Exception as e:
            print(f"Error parsing timestamp: {record.get('timestamp', 'N/A')} - {e}")
            record['session_id'] = None
            continue

        if previous_timestamp is None or (current_timestamp - previous_timestamp) > session_timeout:
            session_id += 1

        record['session_id'] = session_id
        previous_timestamp = current_timestamp

    return data

def merge_sessions(data):
    merged_data = []
    grouped = {}

    for record in data:
        tmp_keywords = record.get('tmp_keywords', '')
        if isinstance(tmp_keywords, list):
            tmp_keywords = ','.join(map(str, tmp_keywords))
        key = (record['user_id'], record['url'], record['title'], tmp_keywords, record['session_id'])

        if key not in grouped:
            grouped[key] = []
        grouped[key].append(record)

    for key, records in grouped.items():
        merged_record = records[0].copy()
        merged_record['pageview_count'] = sum(r['pageview_count'] for r in records)
        merged_record['entry_id'] = min(records, key=lambda r: r['timestamp'])['entry_id']
        merged_record['timestamp'] = min(records, key=lambda r: r['timestamp'])['timestamp']

        for field in merged_record.keys():
            for r in records:
                if r.get(field):
                    merged_record[field] = r[field]
                    break

        merged_data.append(merged_record)

    return merged_data


def calculate_raw_time_on_page(data):
    for i in range(len(data) - 1):
        current_timestamp = parser.parse(str(data[i]['timestamp']))
        next_timestamp = parser.parse(str(data[i + 1]['timestamp']))
        raw_time_on_page = (next_timestamp - current_timestamp).total_seconds()
        data[i]['raw_time_on_page'] = raw_time_on_page
    if data:
        data[-1]['raw_time_on_page'] = 1800
    return data

def update_last_record_time(data, next_day_data, MAX_TIME_PER_PAGE=3600):
    if data and next_day_data:
        last_record = data[-1]
        next_record = next_day_data[0]
        raw_time_on_page = (parser.parse(str(next_record['timestamp'])) - parser.parse(str(last_record['timestamp']))).total_seconds()
        last_record['raw_time_on_page'] = raw_time_on_page
        last_record['capped_time_on_page'] = min(raw_time_on_page, MAX_TIME_PER_PAGE)
    return data

def calculate_capped_time_on_page(data, MAX_TIME_PER_PAGE=3600):
    for record in data:
        record['capped_time_on_page'] = min(record['raw_time_on_page'], MAX_TIME_PER_PAGE)
    return data

def add_additional_columns(data, columns):
    current_time = datetime.now()
    current_date = current_time.date()
    updated_data = []

    for record in data:
        if isinstance(record, tuple):
            record = dict(zip(columns, record))
        record['source_name'] = 'clickhouse-streaming-data'
        record['source_id'] = 1
        record['is_update'] = False
        record['is_delete'] = False
        record['created_time'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
        record['created_date'] = current_date.strftime('%Y-%m-%d')
        updated_data.append(record)

    return updated_data

def processing_browsing_history(browsing_data, browing_historty_columns):
    # cleaned_browsing_history_data = remove_duplicates_browsing_history(browsing_data)
    unique_browsing_data = remove_duplicates_browsing_history(browsing_data)
    enhanced_browsing_history_data = add_window_time_columns(unique_browsing_data)
    print(f"Length of enhanced data: {len(enhanced_browsing_history_data)}")
    if enhanced_browsing_history_data:
        first_record = enhanced_browsing_history_data[0]
        print("First record keys:", list(first_record.keys()))
        print("Sample window_time fields:")
        print(f"  window_time_overall: {first_record.get('window_time_overall')}")
        print(f"  window_time_overall_meaning: {first_record.get('window_time_overall_meaning')}")
        print(f"  window_time_details: {first_record.get('window_time_details')}")
        print(f"  window_time_details_meaning: {first_record.get('window_time_details_meaning')}")

    enhanced_browsing_history_data = add_exact_keywords_column(enhanced_browsing_history_data)
    sessioned_browsing_history_data = assign_session_ids(enhanced_browsing_history_data)
    merged_browsing_history_data = merge_sessions(sessioned_browsing_history_data)
    raw_time_data = calculate_raw_time_on_page(merged_browsing_history_data)
    final_browsing_history_data = calculate_capped_time_on_page(raw_time_data)
    final_browsing_history_data = add_additional_columns(final_browsing_history_data, browing_historty_columns)
    final_browsing_history_data_df = pd.DataFrame(final_browsing_history_data)
    return final_browsing_history_data_df


def load(entity_name, input_df):
    # LOAD
    current_date = datetime.now().strftime('%Y%m%d')
    current_datetime = datetime.now().strftime('%Y%m%d%H%M%S')
    
    general_lib.write_dls(input_df, "json", ACCOUNT_NAME, AZURE_DATALAKE_STORAGE_KEY, 
            DESTINATION_CONTAINER, 
            "{}/{}/{}/{}/{}_{}.{}".format(BASE_PATH, entity_name,"json", current_date, current_datetime, entity_name, "json")
        )

def add_additional_columns_df(df: pd.DataFrame) -> pd.DataFrame:
    current_time = datetime.now()
    df = df.copy()  # avoid modifying the original DataFrame

    df['source_name'] = 'clickhouse-streaming-data'
    df['source_id'] = 1
    df['is_update'] = False
    df['is_delete'] = False
    df['created_time'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
    df['created_date'] = current_time.date().strftime('%Y-%m-%d')

    return df


def main():
    browsinghistory_df =incremental_load("browsinghistory")
    browing_historty_columns = browsinghistory_df.columns
    browsing_history_data = list(browsinghistory_df.itertuples(index=False, name=None))
    browsinghistory_df = processing_browsing_history(browsing_history_data, browing_historty_columns)
    load("browsinghistory", browsinghistory_df)

    users_df = incremental_load("users")
    columns = ['user_id', 'username', 'email', 'password', 'created_time_original', 'updated_time',
            'preferred_areas', 'preferred_content_types', 'preferred_learn_style', 'education_lv']
    user_data = list(users_df.itertuples(index=False, name=None))
    user_data = add_additional_columns(user_data, columns)
    users_df = pd.DataFrame(user_data)
    load("users", users_df)
    
    job_date = datetime.now().strftime('%Y%m%d')
    os.makedirs("reports", exist_ok=True)
    
     # ✅ Report for browsinghistory
    qr1 = QualityReport(browsinghistory_df, entity_name="browsinghistory")
    qr1.summarize_volume()
    qr1.check_nulls(required_columns=[
        'user_id', 'username', 'password', 'created_time', 'preferred_content_type',
        'entry_id', 'pageview_count', 'referrer_page', 'search_keyword', 'title', 'url', 'tmp_keywords', 'timestamp'
    ])
    qr1.check_formats(expected_dtypes={
        'entry_id': 'string',
        'timestamp': 'datetime64[ns]',
        'pageview_count': 'int64',
        'search_keyword': 'object',
        'tmp_keywords': 'object'
    })
    qr1.check_default_values(default_values={
        'source_id': 2,
        'is_update': 0,
        'is_delete': 0
    })
    qr1.check_duplicates(dedup_columns=[
        'user_id', 'username', 'password', 'created_time', 'preferred_content_type',
        'entry_id', 'pageview_count', 'referrer_page', 'search_keyword', 'title', 'url', 'tmp_keywords', 'timestamp'
    ])
    qr1.check_array_fields(array_fields=['search_keyword', 'tmp_keywords'])

    # === Track Clean Success Rates ===
    qr1.track_clean_success_rate("title", lambda x: isinstance(x, str) and x.strip() != "")
    qr1.track_clean_success_rate("url", lambda x: isinstance(x, str) and x.startswith("http"))
    qr1.track_clean_success_rate("timestamp", lambda x: pd.notnull(x) and isinstance(x, pd.Timestamp))
    qr1.track_clean_success_rate("pageview_count", lambda x: pd.notnull(x) and isinstance(x, (int, float)) and x >= 0)
    qr1.track_clean_success_rate("referrer_page", lambda x: isinstance(x, str))
    qr1.track_clean_success_rate("tmp_keywords", lambda x: isinstance(x, list) and len(x) > 0)
    qr1.track_clean_success_rate("visible_content", lambda x: isinstance(x, str) and len(x.strip()) > 0)

    source_df_browsing_history = incremental_load("browsinghistory")
    qr1.compare_with_source(source_df_browsing_history, key_cols=['user_id', 'username', 'password', 'created_time', 'preferred_content_type',
    'entry_id', 'pageview_count', 'referrer_page', 'search_keyword', 'title', 'url', 'tmp_keywords', 'timestamp'])
    report1 = qr1.generate()
    report1 = pd.json_normalize(report1).to_dict(orient="records")[0]
    with open(f"reports/quality_report_streaming_preprocessing_browsinghistory/quality_report_streaming_browsinghistory_incremental{job_date}.json", "w", encoding="utf-8") as f:
        json.dump(report1, f, indent=2, ensure_ascii=False, default=str)
    print(f"✅ Quality report saved to: reports/quality_report_streaming_browsinghistory_incremental_{job_date}.json")


    # ✅ Report for users
    qr2 = QualityReport(users_df, entity_name="users")
    qr2.summarize_volume()
    qr2.check_nulls(required_columns=[
        'user_id', 'username', 'password', 'created_time', 'preferred_content_type'
    ])
    qr2.check_formats(expected_dtypes={
        'user_id': 'Int64',
        'username': 'object',
        "password": 'object',
        'created_time': 'datetime64[ns]',
        'preferred_content_type': 'object'
    })
    qr2.check_default_values(default_values={
        'source_id': 2,
        'is_update': 0,
        'is_delete': 0
    })
    qr2.check_duplicates(dedup_columns=['user_id', 'username', 'password'])
    qr2.check_array_fields(array_fields=[
        'preferred_content_type', 'preferred_learn_styles',
        'education_lv', 'preferred_areas'
    ])

    # === Track Clean Success Rates ===
    qr2.track_clean_success_rate('email', lambda x: bool(re.match(r"[^@]+@[^@]+\.[^@]+", str(x))))
    qr2.track_clean_success_rate('preferred_content_types', lambda x: isinstance(x, list) and len(x) > 0)
    qr2.track_clean_success_rate('preferred_learn_styles', lambda x: isinstance(x, list) and len(x) > 0)
    qr2.track_clean_success_rate('preferred_areas', lambda x: isinstance(x, list) and len(x) > 0)
    qr2.track_clean_success_rate('education_lv', lambda x: str(x).lower() in {"none of the above"})

    # === Optional: Compare with source ===
    source_users = incremental_load("users")

    qr2.compare_with_source(source_users, key_cols=[
            'user_id','username', 'password', 'created_time', 'preferred_content_type',
            'source_id','is_update','is_delete','preferred_learn_styles','education_lv', 'preferred_areas'
        ])
    
    report2 = qr2.generate()
    report2 = pd.json_normalize(report2).to_dict(orient="records")[0]
    with open(f"reports/quality_report_streaming_preprocessing_users/quality_report_streaming_users_incremental_{job_date}.json", "w", encoding="utf-8") as f:
        json.dump(report2, f, indent=2, ensure_ascii=False, default=str)
    print(f"✅ Quality report saved to: reports/quality_report_streaming_users_incremental_{job_date}.json")

        
if __name__ == '__main__':
    main()
        
