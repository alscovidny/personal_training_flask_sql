import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, Numeric, MetaData, create_engine, text
import pandas as pd

DATABASE = 'signaturesdtb'
def make_engine(username ='root', host = 'localhost', password = ''):
    return create_engine(f"mysql+pymysql://{username}:{password}@{host}:3306")

def init_db(engine):
    create_str = "CREATE DATABASE IF NOT EXISTS %s ;" % (DATABASE)
    with engine.connect() as conn:
        conn.execute(create_str)
    return DATABASE

def make_table(engine):
    with engine.connect() as conn:

        conn.execute(f"USE {DATABASE};")
        conn.execute(
            text("CREATE TABLE IF NOT EXISTS signatures \
                (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,\
                name VARCHAR(100),\
                MHCI DECIMAL(20,16),\
                MHCII DECIMAL (20,16),\
                Coactivation_molecules DECIMAL(20,16),\
                Effector_cells DECIMAL(20,16),\
                T_cell_traffic DECIMAL(20,16),\
                NK_cells DECIMAL(20,16),\
                T_cells DECIMAL(20,16),\
                B_cells DECIMAL(20,16),\
                M1_signatures DECIMAL(20,16),\
                Th1_signature DECIMAL(20,16),\
                Antitumor_cytokines DECIMAL(20,16),\
                Checkpoint_inhibition DECIMAL(20,16),\
                Treg DECIMAL(20,16),\
                T_reg_traffic DECIMAL(20,16),\
                Neutrophil_signature DECIMAL(20,16),\
                Granulocyte_traffic DECIMAL(20,16),\
                MDSC DECIMAL(20,16),\
                MDSC_traffic DECIMAL(20,16),\
                Macrophages DECIMAL(20,16),\
                Macrophage_DC_traffic DECIMAL(20,16),\
                Th2_signature DECIMAL(20,16),\
                Protumor_cytokines DECIMAL(20,16),\
                CAF DECIMAL(20,16),\
                Matrix DECIMAL(20,16),\
                Matrix_remodeling DECIMAL(20,16),\
                Angiogenesis DECIMAL(20,16),\
                Endothelium DECIMAL(20,16),\
                Proliferation_rate DECIMAL(20,16),\
                EMT_signature DECIMAL(20,16)\
                );")
        )

def dump_db(engine=make_engine()):
    metadata_obj = MetaData(bind=engine)
    signatures_df = pd.read_csv('signatures.tsv', sep ='\t') # чтение исходного tsv-файла с сигнатурами
    signature_columns = list(signatures_df.columns.values[1:])
    signatures_table = Table( # формат таблицы сигнатур для дампа базы данных
        "signatures",
        metadata_obj,
        Column('id', Integer(), primary_key=True, autoincrement=True, nullable=False),
        Column('name', String()),
        *tuple(map(lambda x : Column(x, Numeric(precision=20, scale=16)), signature_columns))
    )
    # engine = create_engine(f"mysql://{username}:{password}@{host}:3306/{database}")

    with engine.connect() as conn:
        for index, row in signatures_df.iterrows():
            col_names = ['id', 'name']
            col_names.extend(signature_columns)
            col_values = [index + 1]
            col_values.extend(row.values)
            data = { x : y for (x,y) in zip(col_names, col_values)}
            conn.execute(sqlalchemy.insert(signatures_table), [data])
