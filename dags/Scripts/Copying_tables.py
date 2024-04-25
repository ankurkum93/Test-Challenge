import sqlite3
from sqlalchemy import create_engine, Column, Integer, String, Date, Numeric, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import pandas as pd
import glob

conn = sqlite3.connect("/opt/airflow/db/test_database.db")
cursor = conn.cursor()

cursor.execute('''CREATE TABLE if not exists header_history (
    contratto_cod VARCHAR(9),
    status_quote VARCHAR(50),
    codice_agente VARCHAR(5),
    data_firma DATE,           
    codice_ordine_sap VARCHAR(10),
    change_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (contratto_cod) REFERENCES header(contratto_cod)
)''')

cursor.execute('''CREATE TABLE if not exists items_history (
    contratto_cod VARCHAR(9),
    numero_annuncio VARCHAR(2),
    contracted_price DECIMAL(18, 2),
    total_discount DECIMAL(18, 2),
    data_fine_prestazione DATE,
    change_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (contratto_cod, numero_annuncio) REFERENCES items(contratto_cod, numero_annuncio)
)''')


cursor.execute('''INSERT INTO header_history (contratto_cod, status_quote, codice_agente, data_firma, codice_ordine_sap)
                  SELECT contratto_cod, status_quote, codice_agente,data_firma, codice_ordine_sap
                  FROM header''')


cursor.execute('''INSERT INTO items_history (contratto_cod, numero_annuncio, contracted_price, total_discount, data_fine_prestazione)
                  SELECT contratto_cod, numero_annuncio, contracted_price, total_discount, data_fine_prestazione
                  FROM items''')

conn.commit()

df1 = pd.read_sql_table('items_history', 'sqlite:////opt/airflow/db/test_database.db')
df2 = pd.read_sql_table('header_history', 'sqlite:////opt/airflow/db/test_database.db')

# Close connection

conn.close()
