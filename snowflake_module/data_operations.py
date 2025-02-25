import os
from textwrap import dedent
from typing import Any, Dict, List, Optional

from pydantic import ValidationError
from .base import SnowflakeObject, logger
from .options import CopyOptions, PutOptions, OptionsModel

class CopyIntoCommand(SnowflakeObject):
    """
    Represents a COPY INTO command.
    """
    def __init__(
        self,
        database: str,
        schema: str,
        table_name: str,
        cursor: Any,
        source: str,
        file_format: Optional[str] = None,
        options: Dict[str, Any] = {},
        copy_options: Dict[str, Any] = {},
        files: Optional[List[str]] = None,
        pattern: Optional[str] = None,
    ) -> None:
        super().__init__(name="", database=database, schema=schema, cursor=cursor, no_name=True)
        self.table_name = table_name.strip()
        self.source = source.strip()
        self.files = files
        self.pattern = pattern
        self.file_format = file_format
        try:
            self.options = OptionsModel.parse_obj(options)
        except ValidationError as e:
            raise ValueError(f"Invalid COPY options: {e}") from e
        try:
            self.copy_options = CopyOptions(**copy_options)
        except ValidationError as e:
            raise ValueError(f"Invalid copy_options: {e}") from e

    def execute(self) -> None:
        sql = self.generate_copy_sql()
        self.execute_sql(sql)
        logger.info(f"COPY INTO command executed for table '{self.table_name}'.")

    def generate_copy_sql(self) -> str:
        copy_into = f"COPY INTO {self.database}.{self.schema}.{self.table_name}"
        from_clause = f"FROM {self.source}"
        files_clause = self._generate_files_clause()
        pattern_clause = f"PATTERN = '{self.pattern}'" if self.pattern else ""
        file_format_clause = f"FILE_FORMAT = {self.file_format}" if self.file_format else ""
        options_sql = "\n".join(self.options.to_sql_options())
        copy_options_sql = "\n".join(self.copy_options.to_sql_options())
        sql_parts = [
            copy_into,
            from_clause,
            files_clause,
            pattern_clause,
            file_format_clause,
            options_sql,
            copy_options_sql,
        ]
        return "\n".join(part for part in sql_parts if part).strip()

    def _generate_files_clause(self) -> str:
        if self.files:
            formatted_files = ", ".join(f"'{file}'" for file in self.files)
            return f"FILES = ({formatted_files})"
        return ""

class PutCommand(SnowflakeObject):
    """
    Represents a PUT command to upload files to a stage.
    """
    def __init__(
        self,
        database: str,
        schema: str,
        cursor: Any,
        stage_name: str,
        options: Dict[str, Any],
    ) -> None:
        super().__init__(name="", database=database, schema=schema, cursor=cursor, no_name=True)
        self.stage_name = stage_name.strip()
        try:
            self.options = PutOptions(**options)
        except ValidationError as e:
            raise ValueError(f"Invalid PUT options: {e}") from e

    def execute(self, directory_path: str) -> None:
        file_paths = self._get_valid_file_paths(directory_path)
        for file_path in file_paths:
            sql = self._generate_put_sql(file_path)
            self.execute_sql(sql)
            logger.info(f"Uploaded file: {file_path}")

    def _generate_put_sql(self, file_path: str) -> str:
        normalized_path = file_path.replace(os.sep, '/')
        put_command = f"PUT 'file://{normalized_path}' '{self.stage_name}'"
        options_sql = "\n".join(self.options.to_sql_options())
        return f"{put_command}\n{options_sql}".strip()

    def _get_valid_file_paths(self, directory_path: str) -> List[str]:
        normalized_path = os.path.normpath(directory_path)
        if not os.path.isdir(normalized_path):
            raise ValueError(f"Invalid directory path: {normalized_path}")
        file_paths = [
            os.path.join(root, file)
            for root, _, files in os.walk(normalized_path)
            for file in files
            if os.path.isfile(os.path.join(root, file))
        ]
        if not file_paths:
            raise ValueError(f"No valid files found in directory: {normalized_path}")
        return file_paths
