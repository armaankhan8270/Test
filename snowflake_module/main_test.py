import logging
from snowflake_module import (
    CSVFileFormat,
    AWSExternalStageParams,
    Stage,
    CopyIntoCommand,
    PutCommand,
    DirectoryTableParams,
)

# Configure logging.
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# Dummy cursor for demonstration (replace with your actual Snowflake cursor).
class DummyCursor:
    def execute(self, sql: str) -> None:
        print("Executing SQL:")
        print(sql)

dummy_cursor = DummyCursor()

def main():
    # --- Example 1: Create a CSV File Format ---
    csv_options = {
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
        "escape_unenclosed_field": None,
        "trim_space": True,
        "field_optionally_enclosed_by": '"',
        "null_if": ["NULL"],
        "error_on_column_count_mismatch": False,
        "replace_invalid_characters": True,
        "empty_field_as_null": True,
        "skip_byte_order_mark": False,
        "encoding": "UTF8",
    }
    csv_format = CSVFileFormat("my_csv", "MY_DB", "PUBLIC", dummy_cursor, csv_options)
    csv_format.create(if_not_exists=True)

    # --- Example 2: Create an AWS External Stage ---
    aws_stage = Stage(
        name="my_aws_stage",
        type="aws",
        file_format="CSV",
        comment="AWS external stage for data load",
        aws_params=AWSExternalStageParams(
            url="s3://my-bucket/data",
            storage_integration="MY_INTEGRATION",
            encryption_type="AWS_SSE_S3",
            master_key="MY_MASTER_KEY",
            kms_key_id="MY_KMS_KEY_ID",
            use_privatelink_endpoint=True
        ),
        directory_params=DirectoryTableParams(
            enable=True,
            refresh_on_create=True,
            auto_refresh=True,
            notification_integration="MY_NOTIFICATION"
        )
    )
    print("\n-- Stage SQL --")
    print(aws_stage.to_sql())

    # --- Example 3: Execute a COPY INTO Command ---
    copy_cmd = CopyIntoCommand(
        database="MY_DB",
        schema="PUBLIC",
        table_name="target_table",
        cursor=dummy_cursor,
        source="@my_aws_stage",
        file_format="'CSV'",
        options={},  # Additional generic options if needed
        copy_options={"on_error": "CONTINUE"},
    )
    copy_cmd.execute()

    # --- Example 4: Execute a PUT Command ---
    put_options = {"auto_compress": True}
    put_cmd = PutCommand("MY_DB", "PUBLIC", dummy_cursor, "my_aws_stage", put_options)
    put_cmd.execute("./data")

if __name__ == "__main__":
    main()
