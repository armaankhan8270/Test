from typing import Any, Dict
from textwrap import dedent

from .base import SnowflakeObject, logger
from .options import (
    CSVFileFormatOptions,
    JSONFileFormatOptions,
    AvroFileFormatOptions,
    ORCFileFormatOptions,
    ParquetFileFormatOptions,
    XMLFileFormatOptions,
)

class FileFormat(SnowflakeObject):
    """
    Base class for file formats.
    """
    def __init__(
        self,
        name: str,
        database: str,
        schema: str,
        cursor: Any,
        format_type: str,
        options: Dict[str, Any]
    ) -> None:
        super().__init__(name, database, schema, cursor)
        self.format_type = format_type.strip()
        self.options: Any = options  # To be replaced in subclasses

    def create(self, if_not_exists: bool = False) -> None:
        """Generate and execute the CREATE FILE FORMAT SQL."""
        sql = self.generate_create_sql(if_not_exists)
        self.execute_sql(sql)
        logger.info(f"File format '{self.name}' created successfully.")

    def generate_create_sql(self, if_not_exists: bool = False) -> str:
        clause = "IF NOT EXISTS " if if_not_exists else ""
        options_sql = "\n".join(self.options.to_sql_options())
        sql = f"""
            CREATE FILE FORMAT {clause}{self.full_name}
            TYPE = '{self.format_type}'
            {options_sql}
        """
        return dedent(sql).strip()

class CSVFileFormat(FileFormat):
    def __init__(self, name: str, database: str, schema: str, cursor: Any, options: Dict[str, Any]) -> None:
        from .options import CSVFileFormatOptions
        super().__init__(name, database, schema, cursor, "CSV", options)
        self.options = CSVFileFormatOptions(**options)

class JSONFileFormat(FileFormat):
    def __init__(self, name: str, database: str, schema: str, cursor: Any, options: Dict[str, Any]) -> None:
        from .options import JSONFileFormatOptions
        super().__init__(name, database, schema, cursor, "JSON", options)
        self.options = JSONFileFormatOptions(**options)

class AvroFileFormat(FileFormat):
    def __init__(self, name: str, database: str, schema: str, cursor: Any, options: Dict[str, Any]) -> None:
        from .options import AvroFileFormatOptions
        super().__init__(name, database, schema, cursor, "AVRO", options)
        self.options = AvroFileFormatOptions(**options)

class ORCFileFormat(FileFormat):
    def __init__(self, name: str, database: str, schema: str, cursor: Any, options: Dict[str, Any]) -> None:
        from .options import ORCFileFormatOptions
        super().__init__(name, database, schema, cursor, "ORC", options)
        self.options = ORCFileFormatOptions(**options)

class ParquetFileFormat(FileFormat):
    def __init__(self, name: str, database: str, schema: str, cursor: Any, options: Dict[str, Any]) -> None:
        from .options import ParquetFileFormatOptions
        super().__init__(name, database, schema, cursor, "PARQUET", options)
        self.options = ParquetFileFormatOptions(**options)

class XMLFileFormat(FileFormat):
    def __init__(self, name: str, database: str, schema: str, cursor: Any, options: Dict[str, Any]) -> None:
        from .options import XMLFileFormatOptions
        super().__init__(name, database, schema, cursor, "XML", options)
        self.options = XMLFileFormatOptions(**options)
