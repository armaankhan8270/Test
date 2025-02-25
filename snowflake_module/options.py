from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Enums for allowed option values
# ---------------------------------------------------------------------------

class CompressionEnum(str, Enum):
    AUTO = "AUTO"
    GZIP = "GZIP"
    BZ2 = "BZ2"
    BROTLI = "BROTLI"
    ZSTD = "ZSTD"
    DEFLATE = "DEFLATE"
    RAW_DEFLATE = "RAW_DEFLATE"
    NONE = "NONE"

class BinaryFormatEnum(str, Enum):
    HEX = "HEX"
    BASE64 = "BASE64"
    UTF8 = "UTF8"

class ParquetCompressionEnum(str, Enum):
    AUTO = "AUTO"
    LZO = "LZO"
    SNAPPY = "SNAPPY"
    NONE = "NONE"

class EncryptionTypeEnum(str, Enum):
    AWS_CSE = "AWS_CSE"
    AWS_SSE_S3 = "AWS_SSE_S3"
    AWS_SSE_KMS = "AWS_SSE_KMS"
    GCS_SSE_KMS = "GCS_SSE_KMS"
    AZURE_CSE = "AZURE_CSE"
    SNOWFLAKE_FULL = "SNOWFLAKE_FULL"
    SNOWFLAKE_SSE = "SNOWFLAKE_SSE"
    NONE = "NONE"

# ---------------------------------------------------------------------------
# Base Options Model - extra keys are forbidden for strict validation.
# ---------------------------------------------------------------------------

class OptionsModel(BaseModel):
    class Config:
        extra = "forbid"

    def to_sql_options(self) -> List[str]:
        """
        Transform validated options into SQL fragments.
        Booleans become TRUE/FALSE; strings get single-quoted.
        Lists are rendered as comma-separated strings.
        """
        sql_parts = []
        for key, value in self.dict(exclude_none=True).items():
            if isinstance(value, bool):
                val_str = "TRUE" if value else "FALSE"
            elif isinstance(value, (int, float)):
                val_str = str(value)
            elif isinstance(value, list):
                formatted_list = ", ".join(f"'{v}'" for v in value)
                val_str = f"({formatted_list})"
            else:
                val_str = f"'{value}'"
            # Special handling: Convert "parquetcompression" to "COMPRESSION"
            key_sql = "COMPRESSION" if key.lower() == "parquetcompression" else key.upper()
            sql_parts.append(f"{key_sql} = {val_str}")
        return sql_parts

# ---------------------------------------------------------------------------
# File Format Options Models
# ---------------------------------------------------------------------------

class CSVFileFormatOptions(OptionsModel):
    compression: Optional[CompressionEnum] = None
    record_delimiter: Optional[str] = None
    field_delimiter: Optional[str] = None
    multi_line: Optional[bool] = None
    file_extension: Optional[str] = None
    parse_header: Optional[bool] = None
    skip_header: Optional[int] = None
    skip_blank_lines: Optional[bool] = None
    date_format: Optional[str] = None
    time_format: Optional[str] = None
    timestamp_format: Optional[str] = None
    binary_format: Optional[BinaryFormatEnum] = None
    escape: Optional[str] = None
    escape_unenclosed_field: Optional[str] = None
    trim_space: Optional[bool] = None
    field_optionally_enclosed_by: Optional[str] = None
    null_if: Optional[List[str]] = None
    error_on_column_count_mismatch: Optional[bool] = None
    replace_invalid_characters: Optional[bool] = None
    empty_field_as_null: Optional[bool] = None
    skip_byte_order_mark: Optional[bool] = None
    encoding: Optional[str] = None

class JSONFileFormatOptions(OptionsModel):
    compression: Optional[CompressionEnum] = None
    date_format: Optional[str] = None
    time_format: Optional[str] = None
    timestamp_format: Optional[str] = None
    binary_format: Optional[BinaryFormatEnum] = None
    trim_space: Optional[bool] = None
    multi_line: Optional[bool] = None
    null_if: Optional[List[str]] = None
    file_extension: Optional[str] = None
    enable_octal: Optional[bool] = None
    allow_duplicate: Optional[bool] = None
    strip_outer_array: Optional[bool] = None
    strip_null_values: Optional[bool] = None
    replace_invalid_characters: Optional[bool] = None
    ignore_utf8_errors: Optional[bool] = None
    skip_byte_order_mark: Optional[bool] = None

class AvroFileFormatOptions(OptionsModel):
    compression: Optional[CompressionEnum] = None
    trim_space: Optional[bool] = None
    replace_invalid_characters: Optional[bool] = None
    null_if: Optional[List[str]] = None

class ORCFileFormatOptions(OptionsModel):
    trim_space: Optional[bool] = None
    replace_invalid_characters: Optional[bool] = None
    null_if: Optional[List[str]] = None

class ParquetFileFormatOptions(OptionsModel):
    parquetcompression: Optional[ParquetCompressionEnum] = None
    snappy_compression: Optional[bool] = None
    binary_as_text: Optional[bool] = None
    use_logical_type: Optional[bool] = None
    trim_space: Optional[bool] = None
    use_vectorized_scanner: Optional[bool] = None
    replace_invalid_characters: Optional[bool] = None
    null_if: Optional[List[str]] = None

class XMLFileFormatOptions(OptionsModel):
    compression: Optional[CompressionEnum] = None
    ignore_utf8_errors: Optional[bool] = None
    preserve_space: Optional[bool] = None
    strip_outer_element: Optional[bool] = None
    disable_snowflake_data: Optional[bool] = None
    disable_auto_convert: Optional[bool] = None
    replace_invalid_characters: Optional[bool] = None
    skip_byte_order_mark: Optional[bool] = None

# ---------------------------------------------------------------------------
# Stage Options Model (used by the original "stages" module but now we use
# dedicated parameter classes below)
# ---------------------------------------------------------------------------

class StageOptions(OptionsModel):
    url: Optional[str] = None
    storage_integration: Optional[str] = None
    encryption: Optional[str] = None
    file_format: Optional[str] = None
    comment: Optional[str] = None
    master_key: Optional[str] = None
    kms_key_id: Optional[str] = None
    azure_sas_token: Optional[str] = None
    notification_integration: Optional[str] = None
    use_privatelink_endpoint: Optional[bool] = None
    secure: Optional[bool] = None
    refresh_on_create: Optional[bool] = None
    auto_refresh: Optional[bool] = None
    encryption_type: Optional[EncryptionTypeEnum] = None

# ---------------------------------------------------------------------------
# Data Operation Options Models
# ---------------------------------------------------------------------------

class CopyOptions(OptionsModel):
    on_error: Optional[str] = None
    match_by_column_name: Optional[str] = None
    validation_mode: Optional[str] = None
    purge: Optional[bool] = None
    return_failed_only: Optional[bool] = None
    enforce_length: Optional[bool] = None
    truncate_columns: Optional[bool] = None
    force: Optional[bool] = None
    auto_compress: Optional[bool] = None
    size_limit: Optional[int] = None

class PutOptions(OptionsModel):
    source_compression: Optional[str] = None
    auto_compress: Optional[bool] = None
    overwrite: Optional[bool] = None
    parallel: Optional[int] = None
