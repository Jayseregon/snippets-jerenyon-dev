import fnmatch

from sqlalchemy import MetaData, create_engine, inspect
from sqlalchemy.orm import sessionmaker


class DatabaseManager:
    """Manages PostgreSQL database connections and schema/table operations."""

    def __init__(self, user: str, pwd: str, db_name: str, host: str, port: int = 5432):
        """Initialize database connection with given credentials."""
        self.engine = create_engine(
            f"postgresql+psycopg://{user}:{pwd}@{host}:{port}/{db_name}"
        )
        self.metadata = MetaData()
        self.Session = sessionmaker(bind=self.engine)

    def connect(self):
        """Establish a database session connection."""
        self.session = self.Session()

    def disconnect(self):
        """Close the current session and dispose of the engine."""
        self.session.close()
        self.engine.dispose()

    def schema_generator(self, pattern):
        """Yield schema names matching the given pattern."""
        inspector = inspect(self.engine)
        for schema in inspector.get_schema_names():
            if fnmatch.fnmatch(schema, pattern):
                yield schema

    def table_generator(self, schema_name, table_pattern):
        """Yield table names in the given schema matching the pattern."""
        inspector = inspect(self.engine)
        for table in inspector.get_table_names(schema=schema_name):
            if fnmatch.fnmatch(table, table_pattern):
                yield table

    def dev_get_all_schemas(self):
        """Return a list of all schemas in the database."""
        return list(self.schema_generator("*"))

    def get_all_tables_in_schema(self, target_schema):
        """Return a list of all tables in the specified schema."""
        return list(self.table_generator(target_schema, "*"))

    def get_table_from_pattern(self, schema_pattern, table_pattern):
        """Return a list of (schema, table) tuples matching the given patterns."""
        return [
            (schema, table)
            for schema in self.schema_generator(schema_pattern)
            for table in self.table_generator(
                schema_name=schema, table_pattern=table_pattern
            )
        ]
