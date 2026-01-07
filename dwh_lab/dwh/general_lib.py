# https://stackoverflow.com/questions/78163748/azure-data-lake-gen-2-python-copying-files-within-data-lake-folders
# https://github.com/Azure/azure-data-lake-store-python

import json
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
import itertools

import logging
import sys

# Setup logging
logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.setLevel(logging.INFO)
logger.addHandler(handler)
logger.propagate = True

ACCOUNT_NAME = "13f45cadls"
AZURE_DATALAKE_STORAGE_KEY =  "+oB+LkoL2KPaMvZbChL9vKVr/3lFJyDjmHI2cpyFJFDlMFW2pEzPN1zAQbmx9ovFE0hX1vvfll66+ASthCJINQ=="
# GMAIL_APP_PASSWORD= "fyji hord sosm lpic"
GMAIL_APP_PASSWORD= "VietNamVoDich2610"
CLICK_HOUSE_HOST = "h8tw70myst.ap-northeast-1.aws.clickhouse.cloud"
CLICK_HOUSE_USER = "default"
CLICK_HOUSE_PASSWORD = "YAWU5r~485Xr~"


def convert_uuid(val):
    if isinstance(val, uuid.UUID):
        return str(val)
    return val

def list_folder_path(file_system_name, account_name, account_key, parent_path):
    service_client = get_azure_service_client_by_account_key(account_name, account_key)
    file_system_client = service_client.get_file_system_client(file_system_name)
    paths = file_system_client.get_paths(path=parent_path)

    folders = []
    for p in paths:
        if p.is_directory:
            folder_name = p.name.split("/")[-1]
            folders.append(folder_name)

    if not folders:
        return None
    return folders

def list_file_path(file_system_name, account_name, account_key, parent_path):
    service_client = get_azure_service_client_by_account_key(account_name, account_key)
    file_system_client = service_client.get_file_system_client(file_system_name)
    paths = file_system_client.get_paths(path=parent_path)

    files = []
    for p in paths:
        if not p.is_directory:
            files.append(p.name)  # Full path from container root

    if not files:
        return None
    return files
    

def get_azure_service_client_by_account_key(account_name, account_key) -> DataLakeServiceClient:
    account_url = f"https://{account_name}.dfs.core.windows.net"
    service_client = DataLakeServiceClient(account_url, credential=account_key)
    return service_client

def get_click_house_client(host, user, password):
    return clickhouse_connect.get_client(
    host= host,
    user= user,
    password= password,
    secure=True
)

def read_click_house(click_house_client, query):
    try:
        result = click_house_client.query(query)
        rows = result.result_rows
        columns = result.column_names
        df = pd.DataFrame(rows, columns=columns)
        logger.info("Load success: {} records".format(len(df)))
        return df
    except Exception as e:
        logger.info(f"Error reading file from Click House: {e}")

def read_json_lines_in_chunks(data_buffer, chunk_size=100000):
    rows = []
    for i, line in enumerate(data_buffer):
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError as e:
            logger.warning(f"[⚠️] Failed to parse line {i}: {e}")
        if len(rows) >= chunk_size:
            yield pd.DataFrame(rows)
            rows.clear()
    if rows:
        yield pd.DataFrame(rows)


def read_azure_datalake_storage_all_file_in_folder(file_system_name, folder_path, account_name, account_key):
    service_client = get_azure_service_client_by_account_key(account_name, account_key)
    file_system_client = service_client.get_file_system_client(file_system_name)
    paths = file_system_client.get_paths(path=folder_path)

    dfs = []
    for path in paths:
        if not path.is_directory and path.name.lower().endswith('.json'):
            file_client = file_system_client.get_file_client(path.name)
            download = file_client.download_file()
            file_content = download.readall().decode('utf-8')
            
            data = StringIO(file_content)
            df = pd.read_json(data, lines=True)
            dfs.append(df)
            logger.info(f"Loaded JSON file successfully: {path.name} with {len(df)} records")

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Combined total {len(combined_df)} records from all JSON files in folder {folder_path}")
    else:
        combined_df = pd.DataFrame()
        logger.warning(f"No JSON files found in folder {folder_path}")

    return combined_df

def read_azure_datalake_storage(file_system_name, file_path, account_name, account_key):
    logger.info(f"Loading file {file_system_name}, {file_path}")
    service_client = get_azure_service_client_by_account_key(account_name, account_key)
    file_system_client = service_client.get_file_system_client(file_system_name)
    file_client = file_system_client.get_file_client(file_path)
    download = file_client.download_file(timeout=6000)
    file_content = download.readall().decode('utf-8') 

    # 🔸 New: Skip processing if the file is empty or only whitespace
    if not file_content.strip():
        logger.warning(f"⚠️ Skipped empty file: {file_path}")
        return pd.DataFrame()
    
    # Determine file type by extension
    if file_path.lower().endswith('.json'):
        data = StringIO(file_content)
        try:
            df = pd.read_json(data)  # Try standard JSON array
        except ValueError as e:
            if "Trailing data" in str(e) or "All arrays must be of the same length" in str(e):
                data.seek(0)
                df = pd.read_json(data, lines=True)  # Fall back to JSON Lines
            else:
                raise
        logger.info("Loaded JSON file successfully: {} records".format(len(df)))
    else:
        data = StringIO(file_content)
        df = pd.read_csv(data)
        logger.info("Loaded CSV file successfully: {} records".format(len(df)))

    return df      

def convert_str_bool_columns(df):
    for col in df.columns:
        if df[col].dtype == object:
            try:
                unique_vals = set(df[col].dropna().astype(str).str.lower().unique())
                if unique_vals.issubset({"true", "false"}):
                    df[col] = df[col].astype(str).str.lower().map({"true": True, "false": False})
            except Exception as e:
                print(f"Warning: Skipped column {col} due to error: {e}")
    return df

def send_email(email_recipient, email_subject, email_body_text):
    # ========== 🔧 CONFIGURATION ==========
    your_email = "dbpedia.rs@gmail.com"
    your_app_password = GMAIL_APP_PASSWORD  # Use your Gmail App Password
    recipient_email = email_recipient
    subject = "Hello from Python"
    body_text = "This is a test email sent from a Python script with Gmail SMTP."
    
    # Optional attachment
    attach_file = True
    file_path = Path("report.pdf")  # Replace with your file
    
    # ========== 📧 EMAIL SETUP ==========
    msg = EmailMessage()
    msg['From'] = your_email
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.set_content(body_text)
    
    # ========== 📎 ATTACH FILE (optional) ==========
    # if attach_file and file_path.exists():
    #     with open(file_path, 'rb') as f:
    #         file_data = f.read()
    #         file_name = file_path.name
    #     msg.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_name)
    
    # ========== 📤 SEND EMAIL ==========
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
            smtp.starttls()  # Secure the connection
            smtp.login(your_email, your_app_password)
            smtp.send_message(msg)
        logger.info("✅ Email sent successfully.")
    except Exception as e:
        logger.info("❌ Failed to send email:", e)
    #print("Email sending is disabled in this environment. Please enable it in your local setup.")

def read_chunk_and_writle_dls(source_file_system_name, source_file_path, destination_file_system_name, destination_file_path,
                            account_name, account_key, archive_path):
    service_client = get_azure_service_client_by_account_key(account_name, account_key)
    source_file_system_client = service_client.get_file_system_client(source_file_system_name)
    source_file_client = source_file_system_client.get_file_client(source_file_path)
    dest_file_system_client = service_client.get_file_system_client(destination_file_system_name)
    dest_file_client = dest_file_system_client.get_file_client(destination_file_path)

    archive_file_system_client = service_client.get_file_system_client(source_file_system_name)



    download = source_file_client.download_file(timeout=6000)
    chunks = download.chunks()
    logger.info(f"len chunks: {len(chunks)}")

    # stream = StringIO(download.readinto().decode("utf-8")) 

    total_rows = 0
    total_rows_archive = 0
    offset = 0
    index = 0
    dest_file_client.create_file() 
    for chunk in chunks:
        chunk_str = chunk.decode('utf-8')
        chunk_io = StringIO(chunk_str)
        chunk_df = pd.read_csv(chunk_io)

        df_mem_mb = chunk_df.memory_usage(deep=True).sum() / (1024 ** 2)
        num_rows = len(chunk_df)
        logger.info(chunk_df.head(2))
        # 🔧 Do processing here instead of storing in list
        logger.info(f"🧱 Processing chunk with {num_rows} rows... and {df_mem_mb} size ...")
        total_rows += num_rows
        # process_chunk(chunk)  <-- your custom logic here

        # WRITE TO DLS json
        json_buffer = StringIO()
        #chunk_df.to_json(json_buffer, orient="records", lines=True, force_ascii=False, default_handler=str)
        import math

        MAX_BYTES = 2 * 1024 * 1024  # 2MB per append (safe upper limit)

        offset = 0
        buffer = StringIO()
        rows_written = 0

        for i, row in input_df.iterrows():
            buffer.write(json.dumps(row, ensure_ascii=False, default=str) + '\n')
            rows_written += 1
            if buffer.tell() >= MAX_BYTES:
                data_bytes = buffer.getvalue().encode('utf-8')
                file_client.append_data(data=data_bytes, offset=offset, length=len(data_bytes))
                offset += len(data_bytes)
                buffer = StringIO()  # reset buffer

        # Final flush for remaining rows
        if buffer.tell() > 0:
            data_bytes = buffer.getvalue().encode('utf-8')
            file_client.append_data(data=data_bytes, offset=offset, length=len(data_bytes))
            offset += len(data_bytes)

        file_client.flush_data(offset)

        json_data = json_buffer.getvalue()
        # Encode to bytes for correct length count
        data_bytes = json_data.encode("utf-8")
        dest_file_client.append_data(data=data_bytes, offset=offset, length=len(data_bytes))
        offset += len(data_bytes)
        dest_file_client.flush_data(offset)

        # archive
        chunk_archive_path = archive_path.replace('.parquet', f'_{index}.parquet')
        archive_file_client = archive_file_system_client.get_file_client(chunk_archive_path)
        archive_file_client.create_file()

        parquet_buffer = BytesIO()
        chunk_df = chunk_df.astype({col: str for col in chunk_df.select_dtypes(include='object').columns})
        chunk_df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        parquet_buffer.seek(0)
        parquet_data = parquet_buffer.read()
        archive_file_client.append_data(data=parquet_data, offset=0,  length=len(parquet_data),timeout=6000)
        archive_file_client.flush_data(len(parquet_data),timeout=6000)
        total_rows_archive += num_rows

        # index += 1
        # if index == 2:
        #     break

        index += 1
        logger.info(f"✅ Index processed: {index}")
        logger.info(f"✅ Total processed: {total_rows} rows")
        if index >= 4:
            break
        


    logger.info(f"✅ Total processed: {total_rows} rows")
    logger.info(f"✅ Total archived: {total_rows_archive} rows")


def write_dls(input_df, format, account_name, account_key, file_system_name, file_path, chunk_size=25000):
    logger.info("Loading to: {}/{}".format(file_system_name, file_path))
    service_client = get_azure_service_client_by_account_key(account_name, account_key)
    file_system_client = service_client.get_file_system_client(file_system_name)
    file_client = file_system_client.get_file_client(file_path)
    file_client.create_file()  # This will overwrite if the file exists
    # add new comment

    if format == "csv":
        csv_buffer = StringIO()
        input_df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        file_client.append_data(data=csv_data, offset=0, length=len(csv_data), timeout=6000)
        file_client.flush_data(len(csv_data),timeout=6000)
        logger.info(f"✅ CSV written to: {file_path}")

    elif format == "json":
        # Convert entire DataFrame to JSON array
        # json_buffer = StringIO()
        # input_df.to_json(json_buffer, orient="records", lines=False, force_ascii=False, default_handler=str)
        # json_data = json_buffer.getvalue()

        # # Encode to bytes for correct length count
        # data_bytes = json_data.encode("utf-8")
        # file_client.append_data(data=data_bytes, offset=0, length=len(data_bytes))
        # file_client.flush_data(len(data_bytes))
        
        # logger.info(f"✅ JSON array written to {file_path}")


        # new version
        # Convert entire DataFrame to list of dicts
        records = input_df.to_dict(orient="records")
        json_data = json.dumps(records, ensure_ascii=False, separators=(",", ":"), default=str)
        data_bytes = json_data.encode("utf-8")
        file_client.append_data(data=data_bytes, offset=0, length=len(data_bytes))
        file_client.flush_data(len(data_bytes))
        logger.info(f"✅ JSON array written to {file_path}")
        
    elif format == "jsonline":
        offset = 0
        chunks = (input_df[i:i + chunk_size] for i in range(0, len(input_df), chunk_size))
        for idx, chunk in enumerate(chunks):
            json_buffer = StringIO()
            chunk.to_json(json_buffer, orient="records", lines=True, force_ascii=False, default_handler=str)
            json_data = json_buffer.getvalue()

            # Encode to bytes for correct length count
            data_bytes = json_data.encode("utf-8")
            file_client.append_data(data=data_bytes, offset=offset, length=len(data_bytes))
            offset += len(data_bytes)
            logger.info(f"🧱 Chunk {idx+1} written: {len(chunk)} rows")
        
        # json_buffer = StringIO()
        # input_df.to_json(json_buffer, orient="records", lines=True, force_ascii=False, default_handler=str)
        # json_data = json_buffer.getvalue()
        # file_client.append_data(data=json_data, offset=0, length=len(json_data))

        file_client.flush_data(offset)
        logger.info(f"✅ JSON written to {file_path}")
    
        
    elif format == "parquet":
        # Convert DataFrame to parquet in memory
        buffer = BytesIO()
        input_df.to_parquet(buffer, index=False, engine="pyarrow")  # or "fastparquet"
        buffer.seek(0)

        # Upload to ADLS
        file_client.append_data(data=buffer.read(), offset=0, length=buffer.tell())
        file_client.flush_data(buffer.tell())
        logger.info(f"✅ Parquet written to {file_path}")
    else:
        pass
