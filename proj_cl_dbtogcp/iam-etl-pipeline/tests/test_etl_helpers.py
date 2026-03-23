"""
Unit tests for IAM ETL helper utilities.
Run with: pytest tests/ -v
"""
import pytest
import pandas as pd
import pyarrow.parquet as pq
import io
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ── Import helpers directly (no Airflow context needed) ───────────────────────
# We import the module-level functions that don't depend on hooks/variables
from dags.iam_etl_dag import add_etl_metadata, df_to_parquet_bytes, BQ_SCHEMAS, IAM_TABLES


class TestAddEtlMetadata:
    def test_adds_loaded_at_column(self):
        df = pd.DataFrame({"user_id": ["1", "2"], "username": ["alice", "bob"]})
        result = add_etl_metadata(df, source="postgres/iam")
        assert "_etl_loaded_at" in result.columns

    def test_adds_source_column(self):
        df = pd.DataFrame({"user_id": ["1"]})
        result = add_etl_metadata(df, source="oracle/iam")
        assert result["_etl_source"].iloc[0] == "oracle/iam"

    def test_normalises_column_names_to_lowercase(self):
        df = pd.DataFrame({"USER_ID": ["1"], "USERNAME": ["alice"]})
        result = add_etl_metadata(df, source="oracle/iam")
        assert "user_id" in result.columns
        assert "username" in result.columns
        assert "USER_ID" not in result.columns

    def test_preserves_row_count(self):
        df = pd.DataFrame({"id": range(100)})
        result = add_etl_metadata(df, source="test")
        assert len(result) == 100


class TestDfToParquetBytes:
    def test_returns_bytes(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        result = df_to_parquet_bytes(df)
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_parquet_is_readable(self):
        df = pd.DataFrame({"user_id": ["u1"], "email": ["test@example.com"]})
        parquet_bytes = df_to_parquet_bytes(df)
        buf = io.BytesIO(parquet_bytes)
        read_back = pq.read_table(buf).to_pandas()
        assert list(read_back["user_id"]) == ["u1"]
        assert list(read_back["email"]) == ["test@example.com"]

    def test_empty_dataframe(self):
        df = pd.DataFrame({"a": pd.Series([], dtype=str)})
        result = df_to_parquet_bytes(df)
        assert isinstance(result, bytes)


class TestBqSchemas:
    def test_all_tables_have_etl_columns(self):
        for table_name, schema in BQ_SCHEMAS.items():
            field_names = [f["name"] for f in schema]
            assert "_etl_loaded_at" in field_names, f"{table_name} missing _etl_loaded_at"
            assert "_etl_source" in field_names, f"{table_name} missing _etl_source"

    def test_required_fields_have_correct_mode(self):
        for table_name, schema in BQ_SCHEMAS.items():
            for field in schema:
                if field["name"] in ("_etl_loaded_at", "_etl_source"):
                    assert field["mode"] == "REQUIRED", (
                        f"{table_name}.{field['name']} should be REQUIRED"
                    )


class TestIamTablesConfig:
    def test_postgres_tables_have_required_keys(self):
        required_keys = {"name", "query", "partition_field", "bq_table"}
        for tbl in IAM_TABLES["postgres"]:
            assert required_keys.issubset(set(tbl.keys())), (
                f"Table {tbl.get('name')} missing keys"
            )

    def test_oracle_tables_have_required_keys(self):
        required_keys = {"name", "query", "partition_field", "bq_table"}
        for tbl in IAM_TABLES["oracle"]:
            assert required_keys.issubset(set(tbl.keys()))

    def test_all_bq_tables_have_schema(self):
        all_tables = (
            [t["bq_table"] for t in IAM_TABLES["postgres"]]
            + [t["bq_table"] for t in IAM_TABLES["oracle"]]
        )
        for bq_table in all_tables:
            assert bq_table in BQ_SCHEMAS, f"No BQ schema defined for {bq_table}"

    def test_queries_contain_watermark_placeholder(self):
        for tbl in IAM_TABLES["postgres"] + IAM_TABLES["oracle"]:
            assert "{watermark}" in tbl["query"], (
                f"{tbl['name']} query missing {{watermark}} placeholder"
            )
