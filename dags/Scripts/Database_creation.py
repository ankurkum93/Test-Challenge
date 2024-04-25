import sqlite3
from sqlalchemy import create_engine, Column, Integer, String, Date, Numeric, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import pandas as pd
import glob


conn = sqlite3.connect("/opt/airflow/db/test_database.db")
cursor = conn.cursor()

# Create header table
cursor.execute('''CREATE TABLE if not exists header (
                contratto_cod VARCHAR(9) PRIMARY KEY,
                codice_ordine_sap VARCHAR(10),
                tipo_contratto VARCHAR(6) NOT NULL,
                codice_opec VARCHAR(8) NOT NULL,
                data_firma DATE,
                net_amount DECIMAL(18, 2) NOT NULL,
                causale_annullamento VARCHAR(50),
                data_annullamento DATE,
                codice_agente VARCHAR(5) NOT NULL,
                status_quote VARCHAR(50) NOT NULL,
                creazione_dta DATE NOT NULL
                )''')

# Create items table
cursor.execute('''CREATE TABLE if not exists items (
                contratto_cod VARCHAR(9),
                numero_annuncio VARCHAR(2),
                list_total DECIMAL(18, 2) NOT NULL,
                contracted_price DECIMAL(18, 2),
                total_discount DECIMAL(18, 2),
                data_attivazione DATE,
                data_fine_prestazione DATE,
                product_code VARCHAR(50) NOT NULL,
                quantity INTEGER NOT NULL,
                causale_annullamento VARCHAR(50),
                data_annullamento DATE,
                status_item VARCHAR(1) NOT NULL,
                creazione_dta DATE NOT NULL,
                FOREIGN KEY (contratto_cod) REFERENCES header(contratto_cod)
                )''')

# Commit changes and close connection
conn.commit()

Base = declarative_base()


class Header(Base):
    __tablename__ = 'header'
    contratto_cod = Column(String(9), primary_key=True)
    codice_ordine_sap = Column(String(10))
    tipo_contratto = Column(String(6), nullable=False)
    codice_opec = Column(String(8), nullable=False)
    data_firma = Column(Date)
    net_amount = Column(Numeric(precision=18, scale=2), nullable=False)
    causale_annullamento = Column(String(50))
    data_annullamento = Column(Date)
    codice_agente = Column(String(5), nullable=False)
    status_quote = Column(String(50), nullable=False)
    creazione_dta = Column(Date, nullable=False)

class Items(Base):
    __tablename__ = 'items'        
    contratto_cod = Column(String(9), ForeignKey('header.contratto_cod'), primary_key=True)
    numero_annuncio = Column(String(2), nullable=False)
    list_total = Column(Numeric(precision=18, scale=2), nullable=False)
    contracted_price = Column(Numeric(precision=18, scale=2))
    total_discount = Column(Numeric(precision=18, scale=2))
    data_attivazione = Column(Date)
    data_fine_prestazione = Column(Date)
    product_code = Column(String(50), nullable=False)
    quantity = Column(Integer, nullable=False)
    causale_annullamento = Column(String(50))
    data_annullamento = Column(Date)
    status_item = Column(String(1), nullable=False)
    creazione_dta = Column(Date, nullable=False)

# Create an engine and tables
engine = create_engine('sqlite:////opt/airflow/db/test_database.db')
Base.metadata.create_all(engine)

def read_file(path_to_the_folder :str):
    read_file = glob.glob(path_to_the_folder)
    return read_file

def items_dataframe(path_to_the_folder :str ):
    dataframe_item = pd.DataFrame()
    read_files = read_file(path_to_the_folder)
    for file in read_files:
        df = pd.read_csv(file,sep= '|')
        dataframe_item = dataframe_item.append(df, ignore_index=True)
    return dataframe_item

def convert_to_datetime(dataframe, **kwargs):
    for col in kwargs:
        dataframe[col] = pd.to_datetime(dataframe[col])
    return dataframe

def df_to_sql(dataframe, table_name :str):
    dataframe.to_sql(table_name, conn, if_exists= 'replace', index= False)
    return dataframe   

def datetime_conversion():
    item = items_dataframe(path_to_the_folder = "/opt/airflow/db/items/*.txt")
    converted_item = convert_to_datetime(item.copy(), data_attivazione = "data_attivazione", data_fine_prestazione = "data_fine_prestazione", creazione_dta = "creazione_dta" )
    header = items_dataframe(path_to_the_folder = "/opt/airflow/db/header/*.txt")
    converted_header = convert_to_datetime(header.copy(), data_firma="data_firma", creazione_dta="creazione_dta")

    return converted_item, converted_header

def converting_to_sql():
    converted_item, _= datetime_conversion()
    _, converted_header  = datetime_conversion()
    df_to_sql(converted_item, 'items')
    df_to_sql(converted_header, 'header')
    conn.close()
    
    return None

if __name__ == "__main__":
    converting_to_sql()



