import pandas as pd
import numpy as np
import json
from datetime import datetime

class QualityReport:
    def __init__(self, df: pd.DataFrame, entity_name: str):
        self.entity_name = entity_name
        self.df = self.flatten_if_json(df)
        self.report = {
            "entity": entity_name,
            "timestamp": datetime.now().isoformat(),
            "null_check": {},
            "format_check": {},
            "default_values_check": {},
            "deduplication_check": {
                "status": "not_checked",
                "duplicate_rows": None,
                "note": None
            },
            "compare_with_source": [],
            "array_fields_check": {},
            "column_mapping_check": {},
            "volume_summary": {
                "num_rows": 0,
                "num_columns": 0,
                "estimated_memory_MB": 0.0
            },
            "checks": [],
            "notes": []
        }

    def flatten_if_json(self, df):
        if isinstance(df, pd.DataFrame) and df.shape[1] == 1:
            first_col = df.columns[0]
            if df[first_col].apply(lambda x: isinstance(x, (dict, list))).all():
                try:
                    flat_df = pd.json_normalize(df[first_col])
                    return flat_df
                except Exception as e:
                    self.report["notes"].append(f"JSON flattening failed: {e}")
                    return df
        elif isinstance(df, list):
            try:
                return pd.json_normalize(df)
            except Exception as e:
                self.report["notes"].append(f"JSON flattening failed: {e}")
        return df

    def check_nulls(self, required_columns: list):
        for col in required_columns:
            value = "missing" if col not in self.df.columns else int(self.df[col].isnull().sum())
            self.report["null_check"][col] = value

    def check_formats(self, expected_dtypes: dict):
        for col, expected_type in expected_dtypes.items():
            actual_type = str(self.df[col].dtype) if col in self.df.columns else None
            match = expected_type == actual_type
            self.report["format_check"][col] = {
                "expected": expected_type,
                "actual": actual_type,
                "match": match,
                "note": None if col in self.df.columns else f"Column '{col}' not found"
            }

    def check_default_values(self, default_values: dict):
        for col, expected_val in default_values.items():
            if col in self.df.columns:
                incorrect = (self.df[col] != expected_val).sum()
                self.report["default_values_check"][col] = int(incorrect)
            else:
                self.report["default_values_check"][col] = "missing"

    def check_duplicates(self, dedup_columns: list):
        missing = [col for col in dedup_columns if col not in self.df.columns]
        
        if not missing:
            # Convert list-type columns to strings for deduplication
            for col in dedup_columns:
                if self.df[col].apply(lambda x: isinstance(x, list)).any():
                    self.df[col] = self.df[col].apply(lambda x: str(x) if isinstance(x, list) else x)

            duplicate_count = int(self.df.duplicated(subset=dedup_columns).sum())
            self.report["deduplication_check"] = {
                "status": "checked",
                "duplicate_rows": duplicate_count,
                "note": None
            }
        else:
            self.report["deduplication_check"] = {
                "status": "skipped",
                "duplicate_rows": None,
                "note": f"Missing columns: {missing}"
            }


    def check_array_fields(self, array_fields: list):
        for col in array_fields:
            if col in self.df.columns:
                invalid_count = self.df[col].apply(lambda x: not isinstance(x, list)).sum()
                self.report["array_fields_check"][col] = int(invalid_count)
            else:
                self.report["array_fields_check"][col] = "missing"

    def check_column_mapping(self, column_mapping: dict):
        for src_col, tgt_col in column_mapping.items():
            self.report["column_mapping_check"][src_col] = tgt_col in self.df.columns

    def summarize_volume(self):
        self.report["volume_summary"] = {
            "num_rows": self.df.shape[0],
            "num_columns": self.df.shape[1],
            "estimated_memory_MB": round(self.df.memory_usage(deep=True).sum() / 1024**2, 2)
        }

    def compare_with_source(self, source_df, key_cols):
        if "compare_with_source" not in self.report:
            self.report["compare_with_source"] = []

        if not isinstance(source_df, pd.DataFrame) or source_df.empty:
            self.report["compare_with_source"].append("Source DataFrame is empty or invalid.")
            return

        if key_cols is None or len(key_cols) == 0:
            self.report["compare_with_source"].append("No key columns provided for comparison.")
            return

        # Check which columns from source are missing in the transformed dataframe
        missing_cols = [col for col in key_cols if col not in self.df.columns]
        if missing_cols:
            self.report["compare_with_source"].append(f"Missing columns in source_df: {missing_cols}")


    def track_clean_success_rate(self, col, valid_fn):
        if col in self.df.columns:
            valid_count = self.df[col].apply(valid_fn).sum()
            total = self.df[col].notnull().sum()
            rate = round(valid_count / total * 100, 2) if total > 0 else None
            note = None
        else:
            valid_count = total = rate = None
            note = f"Column '{col}' not found in dataset"

        self.report["checks"].append({
            "check": f"clean_success_rate_{col}",
            "result": {
                "valid_count": int(valid_count) if valid_count is not None else None,
                "total_checked": int(total) if total is not None else None,
                "success_rate_%": rate,
                "note": note
            }
        })


    def log_etl_errors(self, error_messages):
        for msg in error_messages:
            self.report["notes"].append(f"ETL Error: {msg}")

    def generate(self):
        return self.report
