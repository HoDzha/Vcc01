from dotenv import load_dotenv

from app.config import get_database_config
from app.db import DatabaseClient


def main() -> None:
    load_dotenv()
    db = DatabaseClient(get_database_config())
    columns = db.get_schema_columns()
    print(f"OK: loaded {len(columns)} columns from database schema.")
    print(db.get_schema_overview_text())


if __name__ == "__main__":
    main()
