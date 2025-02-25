import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

class SnowflakeError(Exception):
    """Custom exception for Snowflake operations."""
    pass

class SnowflakeObject:
    """
    Base class for all Snowflake objects.
    
    Provides basic initialization and SQL execution functionality.
    """
    def __init__(
        self,
        name: Optional[str],
        database: str,
        schema: str,
        cursor: Any,
        no_name: bool = False
    ) -> None:
        self.name = name.strip() if name else None
        self.database = database.strip()
        self.schema = schema.strip()
        self.cursor = cursor

        if not hasattr(self.cursor, "execute"):
            raise ValueError("Cursor must have an 'execute' method.")

        required_attrs = ["database", "schema"] if no_name else ["name", "database", "schema"]
        for attr in required_attrs:
            if not getattr(self, attr):
                raise ValueError(f"{attr.capitalize()} cannot be empty.")

    @property
    def full_name(self) -> str:
        """
        Returns the fully qualified name. If this is a no‑name object,
        it returns "database.schema".
        """
        return f"{self.database}.{self.schema}.{self.name}" if self.name else f"{self.database}.{self.schema}"

    def execute_sql(self, sql: str) -> None:
        """
        Executes the provided SQL while logging the command and any errors.
        """
        sql = sql.strip()
        try:
            logger.info(f"Executing SQL:\n{sql}")
            self.cursor.execute(sql)
            logger.info("SQL executed successfully.")
        except Exception as exc:
            logger.error(f"SQL execution failed: {exc}", exc_info=True)
            raise SnowflakeError(f"SQL execution failed: {exc}") from exc
