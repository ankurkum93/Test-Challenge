import unittest
import sqlite3

class TestDataContractCompliance(unittest.TestCase):
    def setUp(self):
        # Connect to the database
        self.conn = sqlite3.connect("/opt/airflow/db/test_database.db")
        self.cursor = self.conn.cursor()

    def test_header_fields_compliance(self):
        
        self.cursor.execute("PRAGMA table_info(header_history);")
        columns = self.cursor.fetchall()
        column_names = [col[1] for col in columns]
        self.assertIn('status_quote', column_names)
        self.assertIn('codice_agente', column_names)
        self.assertIn('codice_ordine_sap', column_names)

    def test_items_fields_compliance(self):
        
        self.cursor.execute("PRAGMA table_info(items_history);")
        columns = self.cursor.fetchall()
        column_names = [col[1] for col in columns]
        self.assertIn('contracted_price', column_names)
        self.assertIn('total_discount', column_names)
        self.assertIn('data_fine_prestazione', column_names)

    def tearDown(self):
        # Close the database connection
        self.conn.close()

if __name__ == "__main__":
    unittest.main()
