import sqlite3
from sqlalchemy import create_engine, Column, Integer, String, Date, Numeric, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import pandas as pd
import os

conn = sqlite3.connect("/opt/airflow/db/test_database.db")
cursor = conn.cursor()


def creating_new_folder():
    current_path = '/opt/airflow/db/Results'
    if os.path.exists(current_path):
        pass
    else:
        os.mkdir(current_path)

    return None


def query_and_save_to_scv(query, name_of_csv):
    df= pd.read_sql(query, conn)
    creating_new_folder()
    df.to_csv(f'/opt/airflow/db/Results/{name_of_csv}.csv')
    return None

if __name__ == "__main__":
    Data_Modelling_First_query = query_and_save_to_scv('SELECT * FROM header_history WHERE contratto_cod = "Y03998230" AND data_firma <= "2022-12-25" ORDER BY data_firma DESC LIMIT 1', "first")
    Data_Modelling_Second_query = query_and_save_to_scv('SELECT contratto_cod, COUNT(*) AS variazioni FROM header_history WHERE contratto_cod = "Y03998230" GROUP BY contratto_cod', "Second")
    Data_Analysis_Second_query = query_and_save_to_scv('SELECT h.contratto_cod FROM header_history h LEFT JOIN items_history i ON h.contratto_cod = i.contratto_cod WHERE i.contratto_cod IS NULL', "Third")
    Data_Analysis_Third_query = query_and_save_to_scv('SELECT i.product_code FROM items i LEFT JOIN header h ON i.contratto_cod = h.contratto_cod WHERE i.status_item= "L"', "Fourth")


conn.close()