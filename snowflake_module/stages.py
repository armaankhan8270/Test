from typing import Optional, Dict
from pydantic import BaseModel, Field, ValidationError
from textwrap import dedent

# ------------------------------------------------------------------------------
# Dedicated Stage Parameter Models using Pydantic
# ------------------------------------------------------------------------------

class InternalStageParams(BaseModel):
    encryption_type: Optional[Literal['SNOWFLAKE_FULL', 'SNOWFLAKE_SSE']] = None

class DirectoryTableParams(BaseModel):
    enable: Optional[bool] = False
    refresh_on_create: Optional[bool] = False
    auto_refresh: Optional[bool] = False
    notification_integration: Optional[str] = None

class AWSExternalStageParams(BaseModel):
    url: str
    storage_integration: Optional[str] = None
    aws_key_id: Optional[str] = None
    aws_secret_key: Optional[str] = None
    aws_token: Optional[str] = None
    aws_role: Optional[str] = None
    encryption_type: Optional[Literal['AWS_CSE', 'AWS_SSE_S3', 'AWS_SSE_KMS', 'NONE']] = 'NONE'
    master_key: Optional[str] = None
    kms_key_id: Optional[str] = None
    use_privatelink_endpoint: Optional[bool] = False

class GCPExternalStageParams(BaseModel):
    url: str
    storage_integration: Optional[str] = None
    encryption_type: Optional[Literal['GCS_SSE_KMS', 'NONE']] = 'NONE'
    kms_key_id: Optional[str] = None

class AzureExternalStageParams(BaseModel):
    url: str
    storage_integration: Optional[str] = None
    azure_sas_token: Optional[str] = None
    encryption_type: Optional[Literal['AZURE_CSE', 'NONE']] = 'NONE'
    master_key: Optional[str] = None
    use_privatelink_endpoint: Optional[bool] = False

# ------------------------------------------------------------------------------
# Top-Level Stage Model
# ------------------------------------------------------------------------------

from typing import Literal

class Stage(BaseModel):
    name: str
    type: Literal['internal', 'aws', 'gcp', 'azure']
    file_format: Optional[str] = None
    comment: Optional[str] = None
    directory_params: Optional[DirectoryTableParams] = None
    internal_params: Optional[InternalStageParams] = None
    aws_params: Optional[AWSExternalStageParams] = None
    gcp_params: Optional[GCPExternalStageParams] = None
    azure_params: Optional[AzureExternalStageParams] = None

    def to_sql(self) -> str:
        options = []
        if self.file_format:
            options.append(f"FILE_FORMAT = '{self.file_format}'")
        if self.comment:
            options.append(f"COMMENT = '{self.comment}'")
        if self.type == "internal" and self.internal_params:
            if self.internal_params.encryption_type:
                options.append(f"ENCRYPTION = '{self.internal_params.encryption_type}'")
        elif self.type == "aws" and self.aws_params:
            options.append(f"URL = '{self.aws_params.url}'")
            if self.aws_params.storage_integration:
                options.append(f"STORAGE_INTEGRATION = '{self.aws_params.storage_integration}'")
            if self.aws_params.encryption_type:
                options.append(f"ENCRYPTION_TYPE = '{self.aws_params.encryption_type}'")
            if self.aws_params.master_key:
                options.append(f"MASTER_KEY = '{self.aws_params.master_key}'")
            if self.aws_params.kms_key_id:
                options.append(f"KMS_KEY_ID = '{self.aws_params.kms_key_id}'")
            if self.aws_params.use_privatelink_endpoint:
                options.append("USE_PRIVATELINK_ENDPOINT = TRUE")
        elif self.type == "gcp" and self.gcp_params:
            options.append(f"URL = '{self.gcp_params.url}'")
            if self.gcp_params.storage_integration:
                options.append(f"STORAGE_INTEGRATION = '{self.gcp_params.storage_integration}'")
            if self.gcp_params.encryption_type:
                options.append(f"ENCRYPTION_TYPE = '{self.gcp_params.encryption_type}'")
            if self.gcp_params.kms_key_id:
                options.append(f"KMS_KEY_ID = '{self.gcp_params.kms_key_id}'")
        elif self.type == "azure" and self.azure_params:
            options.append(f"URL = '{self.azure_params.url}'")
            if self.azure_params.storage_integration:
                options.append(f"STORAGE_INTEGRATION = '{self.azure_params.storage_integration}'")
            if self.azure_params.azure_sas_token:
                options.append(f"AZURE_SAS_TOKEN = '{self.azure_params.azure_sas_token}'")
            if self.azure_params.encryption_type:
                options.append(f"ENCRYPTION_TYPE = '{self.azure_params.encryption_type}'")
            if self.azure_params.master_key:
                options.append(f"MASTER_KEY = '{self.azure_params.master_key}'")
            if self.azure_params.use_privatelink_endpoint:
                options.append("USE_PRIVATELINK_ENDPOINT = TRUE")
        if self.directory_params:
            if self.directory_params.enable:
                options.append("DIRECTORY_ENABLE = TRUE")
            if self.directory_params.refresh_on_create:
                options.append("REFRESH_ON_CREATE = TRUE")
            if self.directory_params.auto_refresh:
                options.append("AUTO_REFRESH = TRUE")
            if self.directory_params.notification_integration:
                options.append(f"NOTIFICATION_INTEGRATION = '{self.directory_params.notification_integration}'")
        options_sql = "\n".join(options)
        return f"CREATE STAGE {self.name}\n{options_sql}"

# ------------------------------------------------------------------------------
# (Optional) Example usage within the module for testing
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        aws_stage = Stage(
            name="my_aws_stage",
            type="aws",
            file_format="CSV",
            comment="AWS external stage for loading data",
            aws_params=AWSExternalStageParams(
                url="s3://my-bucket/data",
                storage_integration="MY_INTEGRATION",
                encryption_type="AWS_SSE_S3",
                master_key="MY_MASTER_KEY",
                kms_key_id="MY_KMS_KEY_ID",
                use_privatelink_endpoint=True
            )
        )
        print("Stage validated successfully!")
        print(aws_stage.to_sql())
    except Exception as e:
        print("Validation error:", e)
