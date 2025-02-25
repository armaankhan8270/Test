import logging
from snowflake_module import (
    CSVFileFormat,
    JSONFileFormat,
    AvroFileFormat,
    ORCFileFormat,
    ParquetFileFormat,
    XMLFileFormat,
    Stage,
    InternalStageParams,
    DirectoryTableParams,
    AWSExternalStageParams,
    GCPExternalStageParams,
    AzureExternalStageParams,
    CopyIntoCommand,
    PutCommand,
)

# Configure logging to display messages to console.
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# Dummy cursor to simulate SQL execution (replace with real cursor in production).
class DummyCursor:
    def execute(self, sql: str) -> None:
        print("=== Executing SQL ===")
        print(sql)
        print("=====================")

dummy_cursor = DummyCursor()

# Database, schema, table, and folder used in every test.
DB_NAME = "POLARSLED_DB"
SCHEMA_NAME = "DEMO_UPLOAD"
TABLE_NAME = "test"
DATA_FOLDER = "./sample"


# ----------------------------
# File Format Tests (3 cases each)
# ----------------------------

def test_csv_format():
    print("\n=== Testing CSV File Format ===")
    test_cases = [
        {
            "compression": "GZIP",
            "record_delimiter": "\n",
            "field_delimiter": ",",
            "multi_line": True,
            "file_extension": ".csv",
            "parse_header": True,
            "skip_header": 1,
            "skip_blank_lines": True,
            "date_format": "AUTO",
            "time_format": "AUTO",
            "timestamp_format": "AUTO",
            "binary_format": "UTF8",
            "escape": "\\",
            "trim_space": True,
            "field_optionally_enclosed_by": '"',
            "null_if": ["NULL"],
            "replace_invalid_characters": True,
            "empty_field_as_null": True,
            "encoding": "UTF8",
        },
        {
            # Variation: no compression, different delimiters.
            "compression": "NONE",
            "record_delimiter": "\r\n",
            "field_delimiter": ";",
            "multi_line": False,
            "file_extension": ".csv",
            "parse_header": False,
            "skip_header": 0,
            "skip_blank_lines": False,
            "date_format": "YYYY-MM-DD",
            "time_format": "HH24:MI:SS",
            "timestamp_format": "YYYY-MM-DD HH24:MI:SS",
            "binary_format": "BASE64",
            "escape": "",
            "trim_space": False,
            "field_optionally_enclosed_by": "'",
            "null_if": [],
            "replace_invalid_characters": False,
            "empty_field_as_null": False,
            "encoding": "UTF8",
        },
        {
            # Variation: Mixed options.
            "compression": "GZIP",
            "record_delimiter": "\n",
            "field_delimiter": "|",
            "multi_line": True,
            "file_extension": ".csv",
            "parse_header": True,
            "skip_header": 2,
            "skip_blank_lines": True,
            "date_format": "AUTO",
            "time_format": "AUTO",
            "timestamp_format": "AUTO",
            "binary_format": "UTF8",
            "escape": "\\",
            "trim_space": True,
            "field_optionally_enclosed_by": '"',
            "null_if": ["NA", "NULL"],
            "replace_invalid_characters": True,
            "empty_field_as_null": True,
            "encoding": "UTF8",
        },
    ]
    for i, opts in enumerate(test_cases, start=1):
        try:
            fmt = CSVFileFormat(f"csv_test_{i}", DB_NAME, SCHEMA_NAME, dummy_cursor, opts)
            fmt.create(if_not_exists=True)
            print(f"CSV File Format test case {i}: Success!")
        except Exception as e:
            print(f"CSV File Format test case {i}: Error - {e}")

def test_json_format():
    print("\n=== Testing JSON File Format ===")
    test_cases = [
        {
            "compression": "GZIP",
            "date_format": "AUTO",
            "time_format": "AUTO",
            "timestamp_format": "AUTO",
            "binary_format": "UTF8",
            "trim_space": True,
            "multi_line": True,
            "null_if": ["NULL"],
            "file_extension": ".json",
            "enable_octal": False,
            "allow_duplicate": False,
            "strip_outer_array": True,
            "replace_invalid_characters": True,
            "ignore_utf8_errors": False,
            "skip_byte_order_mark": False,
        },
        {
            # Variation: Different settings
            "compression": "NONE",
            "date_format": "YYYY-MM-DD",
            "time_format": "HH24:MI:SS",
            "timestamp_format": "YYYY-MM-DD HH24:MI:SS",
            "binary_format": "HEX",
            "trim_space": False,
            "multi_line": False,
            "null_if": [],
            "file_extension": ".json",
            "enable_octal": True,
            "allow_duplicate": True,
            "strip_outer_array": False,
            "replace_invalid_characters": False,
            "ignore_utf8_errors": True,
            "skip_byte_order_mark": True,
        },
        {
            # Variation: Mixed
            "compression": "GZIP",
            "date_format": "AUTO",
            "time_format": "AUTO",
            "timestamp_format": "AUTO",
            "binary_format": "UTF8",
            "trim_space": True,
            "multi_line": True,
            "null_if": ["NULL", "NA"],
            "file_extension": ".json",
            "enable_octal": False,
            "allow_duplicate": False,
            "strip_outer_array": True,
            "replace_invalid_characters": True,
            "ignore_utf8_errors": False,
            "skip_byte_order_mark": False,
        },
    ]
    for i, opts in enumerate(test_cases, start=1):
        try:
            fmt = JSONFileFormat(f"json_test_{i}", DB_NAME, SCHEMA_NAME, dummy_cursor, opts)
            fmt.create(if_not_exists=True)
            print(f"JSON File Format test case {i}: Success!")
        except Exception as e:
            print(f"JSON File Format test case {i}: Error - {e}")

def test_avro_format():
    print("\n=== Testing Avro File Format ===")
    test_cases = [
        {
            "compression": "GZIP",
            "trim_space": True,
            "replace_invalid_characters": True,
            "null_if": ["NULL"],
        },
        {
            "compression": "NONE",
            "trim_space": False,
            "replace_invalid_characters": False,
            "null_if": [],
        },
        {
            "compression": "BROTLI",
            "trim_space": True,
            "replace_invalid_characters": False,
            "null_if": ["NA"],
        },
    ]
    for i, opts in enumerate(test_cases, start=1):
        try:
            fmt = AvroFileFormat(f"avro_test_{i}", DB_NAME, SCHEMA_NAME, dummy_cursor, opts)
            fmt.create(if_not_exists=True)
            print(f"Avro File Format test case {i}: Success!")
        except Exception as e:
            print(f"Avro File Format test case {i}: Error - {e}")

def test_orc_format():
    print("\n=== Testing ORC File Format ===")
    test_cases = [
        {
            "trim_space": True,
            "replace_invalid_characters": True,
            "null_if": ["NULL"],
        },
        {
            "trim_space": False,
            "replace_invalid_characters": False,
            "null_if": [],
        },
        {
            "trim_space": True,
            "replace_invalid_characters": False,
            "null_if": ["NA", "NULL"],
        },
    ]
    for i, opts in enumerate(test_cases, start=1):
        try:
            fmt = ORCFileFormat(f"orc_test_{i}", DB_NAME, SCHEMA_NAME, dummy_cursor, opts)
            fmt.create(if_not_exists=True)
            print(f"ORC File Format test case {i}: Success!")
        except Exception as e:
            print(f"ORC File Format test case {i}: Error - {e}")

def test_parquet_format():
    print("\n=== Testing Parquet File Format ===")
    test_cases = [
        {
            "parquetcompression": "SNAPPY",
            "snappy_compression": True,
            "binary_as_text": False,
            "use_logical_type": True,
            "trim_space": True,
            "use_vectorized_scanner": True,
            "replace_invalid_characters": True,
            "null_if": ["NULL"],
        },
        {
            "parquetcompression": "AUTO",
            "snappy_compression": False,
            "binary_as_text": True,
            "use_logical_type": False,
            "trim_space": False,
            "use_vectorized_scanner": False,
            "replace_invalid_characters": False,
            "null_if": [],
        },
        {
            "parquetcompression": "SNAPPY",
            "snappy_compression": True,
            "binary_as_text": True,
            "use_logical_type": True,
            "trim_space": True,
            "use_vectorized_scanner": False,
            "replace_invalid_characters": True,
            "null_if": ["NA"],
        },
    ]
    for i, opts in enumerate(test_cases, start=1):
        try:
            fmt = ParquetFileFormat(f"parquet_test_{i}", DB_NAME, SCHEMA_NAME, dummy_cursor, opts)
            fmt.create(if_not_exists=True)
            print(f"Parquet File Format test case {i}: Success!")
        except Exception as e:
            print(f"Parquet File Format test case {i}: Error - {e}")

def test_xml_format():
    print("\n=== Testing XML File Format ===")
    test_cases = [
        {
            "compression": "GZIP",
            "ignore_utf8_errors": False,
            "preserve_space": True,
            "strip_outer_element": False,
            "disable_snowflake_data": False,
            "disable_auto_convert": True,
            "replace_invalid_characters": True,
            "skip_byte_order_mark": False,
        },
        {
            "compression": "NONE",
            "ignore_utf8_errors": True,
            "preserve_space": False,
            "strip_outer_element": True,
            "disable_snowflake_data": True,
            "disable_auto_convert": False,
            "replace_invalid_characters": False,
            "skip_byte_order_mark": True,
        },
        {
            "compression": "GZIP",
            "ignore_utf8_errors": False,
            "preserve_space": True,
            "strip_outer_element": True,
            "disable_snowflake_data": False,
            "disable_auto_convert": True,
            "replace_invalid_characters": True,
            "skip_byte_order_mark": False,
        },
    ]
    for i, opts in enumerate(test_cases, start=1):
        try:
            fmt = XMLFileFormat(f"xml_test_{i}", DB_NAME, SCHEMA_NAME, dummy_cursor, opts)
            fmt.create(if_not_exists=True)
            print(f"XML File Format test case {i}: Success!")
        except Exception as e:
            print(f"XML File Format test case {i}: Error - {e}")

# ----------------------------
# Stage Tests (3 cases each)
# ----------------------------

def test_internal_stage():
    print("\n=== Testing Internal Stage ===")
    test_cases = [
        {
            "name": "internal_stage_1",
            "type": "internal",
            "file_format": "CSV",
            "comment": "Internal stage sample 1",
            "internal_params": {"encryption_type": "SNOWFLAKE_FULL"},
        },
        {
            "name": "internal_stage_2",
            "type": "internal",
            "file_format": "CSV",
            "comment": "Internal stage sample 2",
            "internal_params": {"encryption_type": "SNOWFLAKE_SSE"},
        },
        {
            "name": "internal_stage_3",
            "type": "internal",
            "file_format": "CSV",
            "comment": "Internal stage sample 3",
            "internal_params": {},  # No encryption set
        },
    ]
    for i, params in enumerate(test_cases, start=1):
        try:
            stage_obj = Stage(**params)
            sql = stage_obj.to_sql()
            print(f"Internal Stage test case {i} SQL:")
            print(sql)
            dummy_cursor.execute(sql)
            print(f"Internal Stage test case {i}: Success!")
        except Exception as e:
            print(f"Internal Stage test case {i}: Error - {e}")

def test_aws_stage():
    print("\n=== Testing AWS External Stage ===")
    test_cases = [
        {
            "name": "aws_stage_1",
            "type": "aws",
            "file_format": "CSV",
            "comment": "AWS stage sample 1",
            "aws_params": {
                "url": "s3://bucket1/data",
                "storage_integration": "INT1",
                "encryption_type": "AWS_SSE_S3",
                "master_key": "KEY1",
                "kms_key_id": "KMS1",
                "use_privatelink_endpoint": True,
            },
            "directory_params": {
                "enable": True,
                "refresh_on_create": True,
                "auto_refresh": True,
                "notification_integration": "NTFY1",
            },
        },
        {
            "name": "aws_stage_2",
            "type": "aws",
            "file_format": "CSV",
            "comment": "AWS stage sample 2",
            "aws_params": {
                "url": "s3://bucket2/data",
                "storage_integration": "INT2",
                "encryption_type": "AWS_SSE_KMS",
                "master_key": "KEY2",
                "kms_key_id": "KMS2",
                "use_privatelink_endpoint": False,
            },
        },
        {
            "name": "aws_stage_3",
            "type": "aws",
            "file_format": "CSV",
            "comment": "AWS stage sample 3",
            "aws_params": {
                "url": "s3://bucket3/data",
                "storage_integration": None,
                "encryption_type": "NONE",
                "use_privatelink_endpoint": False,
            },
        },
    ]
    for i, params in enumerate(test_cases, start=1):
        try:
            stage_obj = Stage(**params)
            sql = stage_obj.to_sql()
            print(f"AWS Stage test case {i} SQL:")
            print(sql)
            dummy_cursor.execute(sql)
            print(f"AWS Stage test case {i}: Success!")
        except Exception as e:
            print(f"AWS Stage test case {i}: Error - {e}")

def test_gcp_stage():
    print("\n=== Testing GCP External Stage ===")
    test_cases = [
        {
            "name": "gcp_stage_1",
            "type": "gcp",
            "file_format": "JSON",
            "comment": "GCP stage sample 1",
            "gcp_params": {
                "url": "gs://bucket1/data",
                "storage_integration": "GCP_INT1",
                "encryption_type": "GCS_SSE_KMS",
                "kms_key_id": "GCP_KMS1",
            },
        },
        {
            "name": "gcp_stage_2",
            "type": "gcp",
            "file_format": "JSON",
            "comment": "GCP stage sample 2",
            "gcp_params": {
                "url": "gs://bucket2/data",
                "storage_integration": "GCP_INT2",
                "encryption_type": "NONE",
            },
        },
        {
            "name": "gcp_stage_3",
            "type": "gcp",
            "file_format": "JSON",
            "comment": "GCP stage sample 3",
            "gcp_params": {
                "url": "gs://bucket3/data",
            },
        },
    ]
    for i, params in enumerate(test_cases, start=1):
        try:
            stage_obj = Stage(**params)
            sql = stage_obj.to_sql()
            print(f"GCP Stage test case {i} SQL:")
            print(sql)
            dummy_cursor.execute(sql)
            print(f"GCP Stage test case {i}: Success!")
        except Exception as e:
            print(f"GCP Stage test case {i}: Error - {e}")

def test_azure_stage():
    print("\n=== Testing Azure External Stage ===")
    test_cases = [
        {
            "name": "azure_stage_1",
            "type": "azure",
            "file_format": "JSON",
            "comment": "Azure stage sample 1",
            "azure_params": {
                "url": "azure://container1/data",
                "storage_integration": "AZ_INT1",
                "azure_sas_token": "SAS1",
                "encryption_type": "AZURE_CSE",
                "master_key": "AZ_KEY1",
                "use_privatelink_endpoint": True,
            },
        },
        {
            "name": "azure_stage_2",
            "type": "azure",
            "file_format": "JSON",
            "comment": "Azure stage sample 2",
            "azure_params": {
                "url": "azure://container2/data",
                "storage_integration": "AZ_INT2",
                "encryption_type": "NONE",
            },
        },
        {
            "name": "azure_stage_3",
            "type": "azure",
            "file_format": "JSON",
            "comment": "Azure stage sample 3",
            "azure_params": {
                "url": "azure://container3/data",
                "azure_sas_token": "SAS3",
                "encryption_type": "AZURE_CSE",
                "master_key": "AZ_KEY3",
                "use_privatelink_endpoint": False,
            },
        },
    ]
    for i, params in enumerate(test_cases, start=1):
        try:
            stage_obj = Stage(**params)
            sql = stage_obj.to_sql()
            print(f"Azure Stage test case {i} SQL:")
            print(sql)
            dummy_cursor.execute(sql)
            print(f"Azure Stage test case {i}: Success!")
        except Exception as e:
            print(f"Azure Stage test case {i}: Error - {e}")

# ----------------------------
# Data Operations Tests (3 cases each)
# ----------------------------

def test_copy_into_command():
    print("\n=== Testing COPY INTO Command ===")
    test_cases = [
        {
            "table_name": "test",
            "source": "@aws_stage_1",
            "file_format": "'CSV'",
            "options": {},
            "copy_options": {"on_error": "CONTINUE"},
            "files": ["file1.csv", "file2.csv"],
            "pattern": None,
        },
        {
            "table_name": "test",
            "source": "@gcp_stage_1",
            "file_format": "'JSON'",
            "options": {},
            "copy_options": {"on_error": "SKIP_FILE"},
            "files": None,
            "pattern": ".*\\.json",
        },
        {
            "table_name": "test",
            "source": "@azure_stage_1",
            "file_format": "'XML'",
            "options": {},
            "copy_options": {"on_error": "ABORT_STATEMENT"},
            "files": ["file1.xml"],
            "pattern": None,
        },
    ]
    for i, case in enumerate(test_cases, start=1):
        try:
            copy_cmd = CopyIntoCommand(
                database=DB_NAME,
                schema=SCHEMA_NAME,
                table_name=case["table_name"],
                cursor=dummy_cursor,
                source=case["source"],
                file_format=case["file_format"],
                options=case["options"],
                copy_options=case["copy_options"],
                files=case["files"],
                pattern=case["pattern"],
            )
            copy_cmd.execute()
            print(f"COPY INTO test case {i}: Success!")
        except Exception as e:
            print(f"COPY INTO test case {i}: Error - {e}")

def test_put_command():
    print("\n=== Testing PUT Command ===")
    test_cases = [
        {
            "stage_name": "aws_stage_1",
            "options": {"auto_compress": True},
        },
        {
            "stage_name": "gcp_stage_1",
            "options": {"auto_compress": False, "overwrite": True},
        },
        {
            "stage_name": "azure_stage_1",
            "options": {"auto_compress": True, "parallel": 4},
        },
    ]
    for i, case in enumerate(test_cases, start=1):
        try:
            put_cmd = PutCommand(
                database=DB_NAME,
                schema=SCHEMA_NAME,
                cursor=dummy_cursor,
                stage_name=case["stage_name"],
                options=case["options"],
            )
            put_cmd.execute(DATA_FOLDER)
            print(f"PUT command test case {i}: Success!")
        except Exception as e:
            print(f"PUT command test case {i}: Error - {e}")

# ----------------------------
# Main Test Runner
# ----------------------------

def main():
    print("=== Starting All Tests ===")
    test_csv_format()
    test_json_format()
    test_avro_format()
    test_orc_format()
    test_parquet_format()
    test_xml_format()
    test_internal_stage()
    test_aws_stage()
    test_gcp_stage()
    test_azure_stage()
    test_copy_into_command()
    test_put_command()
    print("=== All Tests Completed ===")

if __name__ == "__main__":
    main()
