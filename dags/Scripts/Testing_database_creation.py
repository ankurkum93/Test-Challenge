import os
import sqlite3
import pandas as pd
import unittest
from Database_creation import converting_to_sql

class TestConvertingToSQL(unittest.TestCase):
    def setUp(self):
        # Create a test database
        self.db_path = "/opt/airflow/db/test_database.db"
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def test_converting_to_sql(self):
        # Call the function to be tested
        converting_to_sql()

        # Check if the tables are created
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.cursor.fetchall()
        table_names = [table[0] for table in tables]
        self.assertIn("header", table_names)
        self.assertIn("items", table_names)

        # Check if the tables are not empty
        self.cursor.execute("SELECT COUNT(*) FROM header;")
        header_count = self.cursor.fetchone()[0]
        self.assertGreater(header_count, 0)

        self.cursor.execute("SELECT COUNT(*) FROM items;")
        items_count = self.cursor.fetchone()[0]
        self.assertGreater(items_count, 0)

    def tearDown(self):
        # Remove the test database
        self.conn.close()
        

if __name__ == "__main__":
    unittest.main()