import os
from typing import Optional, Union

import dotenv
import polars as pl
from alive_progress import alive_bar
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder


def load_tables(
    table_names: Optional[Union[list[str], str]] = None,
) -> Union[dict[str, pl.DataFrame], pl.DataFrame]:
    table_dir = os.path.join(os.environ["BX_SRC_DIR"], "..", "tables")

    if table_names is None:
        table_names = os.listdir(table_dir)

    if isinstance(table_names, str):
        table_names = [table_names]

    for i in range(len(table_names)):
        if not table_names[i].endswith(".parquet"):
            table_names[i] = f"{table_names[i]}.parquet"

    dfs = {}
    for table_name in table_names:
        table_path = os.path.join(table_dir, table_name)
        key = table_name.split(".")[0].lower()
        dfs[key] = pl.read_parquet(table_path)
    if len(dfs) == 1:
        return next(iter(dfs.values()))
    return dfs


def list_tables() -> list[str]:
    query = """SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_type = 'BASE TABLE';"""
    return exec_postgres_query(query)["table_name"].to_list()


def exec_postgres_query(query: str) -> pl.DataFrame:
    with BoilerexamsDatabase() as db:
        with db.engine.connect() as connection:
            return pl.read_database(query=query, connection=connection)


def save_tables(table_names: Union[list[str], str]):
    if isinstance(table_names, str):
        table_names = [table_names]

    with BoilerexamsDatabase() as db:
        with db.engine.connect() as connection, alive_bar(len(table_names)) as bar:
            for table_name in table_names:
                bar.title = f"Saving {table_name} table"
                save_path = os.path.join(
                    os.environ["BX_SRC_DIR"], "..", "tables", f"{table_name}.parquet"
                )
                extra = ""
                if table_name == "Submission":
                    extra = """WHERE "type" = 'MULTIPLE_CHOICE' """
                query = f"""SELECT * FROM public."{table_name}" {extra}"""
                df = pl.read_database(query=query, connection=connection)
                df.write_parquet(save_path)
                bar()


class BoilerexamsDatabase:
    # with context manager
    def __init__(self):
        dotenv.load_dotenv(".env.secret")

        # SSH tunnel configuration
        self.SSH_HOST = os.environ["SSH_HOST"]
        self.SSH_USERNAME = os.environ["SSH_USERNAME"]
        self.SSH_PEM_PATH = os.environ["SSH_PEM_PATH"]
        self.REMOTE_DB_HOST = os.environ["REMOTE_DB_HOST"]
        self.REMOTE_DB_PORT = int(os.environ["REMOTE_DB_PORT"])
        self.LOCAL_BIND_PORT = int(os.environ["LOCAL_BIND_PORT"])

        # Database configuration
        self.DB_USERNAME = os.environ["DB_USERNAME"]
        self.DB_PASSWORD = os.environ["DB_PASSWORD"]
        self.DB_NAME = os.environ["DB_NAME"]

    def __enter__(self):
        self.tunnel = SSHTunnelForwarder(
            (self.SSH_HOST, 22),
            ssh_username=self.SSH_USERNAME,
            ssh_pkey=self.SSH_PEM_PATH,
            remote_bind_address=(self.REMOTE_DB_HOST, self.REMOTE_DB_PORT),
            local_bind_address=("localhost", self.LOCAL_BIND_PORT),
        )
        self.tunnel.start()
        print(f"SSH tunnel established on local port {self.tunnel.local_bind_port}")

        # Construct the database connection string
        self.db_connection_string = f"postgresql://{self.DB_USERNAME}:{self.DB_PASSWORD}@localhost:{self.tunnel.local_bind_port}/{self.DB_NAME}"

        # Create SQLAlchemy engine
        self.engine = create_engine(self.db_connection_string)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tunnel.stop()
        print("SSH tunnel stopped")
