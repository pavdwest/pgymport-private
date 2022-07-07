from typing import List
from xmlrpc.client import Boolean
import psycopg2
import datetime
import os
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2.errors as perrs
import csv


class DbClient():
    '''
    Connects to server and creates specified db if it doesn't exist
    '''
    def __init__(
        self,
        database: str,
        server: str = "localhost",
        username: str = "postgres",
        password: str = "postgres",
        port: int = 5432
    ) -> None:
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.port = port

        # # Connect to server
        self.server_connection = psycopg2.connect(
                host=self.server,
                user=self.username,
                password=self.password
            )

        self.db_connection = self.create_db(database=self.database)


    def create_db(
        self,
        database: str
    ):
        # Set levels
        self.server_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);
        cursor = self.server_connection.cursor()

        # Create table statement
        sql_create_db = f"create database \"{database}\";"

        # Create db
        try:
            cursor.execute(sql_create_db);
        except perrs.DuplicateDatabase as ex:
            print(f"Database '{database}' already exists.")

        # Create db if it doesn't exist
        return psycopg2.connect(
            host=self.server,
            dbname=self.database,
            user=self.username,
            password=self.password
        )

    def get_cursor(self):
        return self.server_connection.cursor()


    def create_table(
        self,
        table: str,
        columns: List[str],
        drop_table_if_exists: Boolean = True
    ):
        # Get cursor object from the database connection
        cursor = self.get_cursor()

        # Drop target table if it already exists
        if drop_table_if_exists:
            sql_drop_existing = f"drop table if exists \"{table}\""
            cursor.execute(sql_drop_existing)
            self.db_connection.commit()
            print(f"Dropped table [{table}].")

        # Create table statement
        cols = [f"\"{c}\"  varchar(256)" for c in columns]
        cols.insert(0, "id BIGSERIAL PRIMARY KEY NOT NULL")     # Add id column
        sql_create_table = ", \n".join(cols)
        sql_create_table = f"create table {table} \n (\n{sql_create_table}\n);"
        # print(sql_create_table)

        # Actually create table
        cursor.execute(sql_create_table)
        self.db_connection.commit()
        print(f"Created table [{table}].")


    def get_file_columns(
        cls,
        filepath: str,
        delimiter: str = ","
    ):
        with open(filepath, "r") as f:
            csv_reader =  csv.DictReader(f, delimiter=delimiter)
            print(csv_reader.fieldnames)
            return csv_reader.fieldnames


    def copy_data_into_db(
        self,
        filepath: str,
        table: str,
        columns: List[str],
        delimiter: str = ","
    ):
        with open(filepath, "r") as f:
            # Skip header
            f.readline()

            # Copy to db
            print(f"Copying file '{filepath}' into db...")
            cursor = self.get_cursor()
            cursor.copy_from(f, table, columns=columns, sep=delimiter)
            self.db_connection.commit()
            self.db_connection.close()
            print("Done.")


    def load_file(
        self,
        filepath: str,
        table: str = None,
        delimiter: str = ","
    ):
        if not os.path.exists(filepath):
            print(f"File not found: {filepath}")
            return

        if not table:
            table = "tmp_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")

        columns = self.get_file_columns(filepath=filepath, delimiter=delimiter)

        self.create_table(
            table=table,
            columns=columns,
            drop_table_if_exists=True
        )

        self.copy_data_into_db(
            filepath=filepath,
            table=table,
            columns=columns,
            delimiter=delimiter
        )
