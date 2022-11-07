from flask import Flask, render_template, request, jsonify
from sqlalchemy import Table, Column, Integer, String, Numeric, MetaData, create_engine, text, select
from searching_test import find_patient, make_describe
from decimal import Decimal
from dump_db import dump_db, init_db, make_table

with open('../password.txt', 'rt') as f:
    username, host, database, password = map(lambda x: x.split()[0], f.readlines())

def make_engine(username ='root', host = 'localhost', database = 'signaturesdtb', password = ''):
    return create_engine(f"mysql+pymysql://{username}:{password}@{host}:3306/{database}")

all_columns = ['mhci', 'mhcii', 'coactivation_molecules', 'effector_cells', 't_cell_traffic', 'nk_cells',
           't_cells', 'b_cells', 'm1_signatures', 'th1_signature', 'antitumor_cytokines', 'checkpoint_inhibition',
           'treg', 't_reg_traffic', 'neutrophil_signature', 'granulocyte_traffic', 'mdsc', 'mdsc_traffic',
           'macrophages', 'macrophage_dc_traffic', 'th2_signature', 'protumor_cytokines', 'caf', 'matrix',
           'matrix_remodeling', 'angiogenesis', 'endothelium', 'proliferation_rate', 'emt_signature'
               ]

engine = make_engine(username = username, host = host, password = password)

signatures_table = Table( # формат таблицы сигнатур для дампа базы данных
    "signatures",
    MetaData(),
    Column('id', Integer(), primary_key=True, autoincrement=True, nullable=False),
    Column('name', String()),
    *tuple(map(lambda x : Column(x, Numeric(precision=20, scale=16)), all_columns))
)

app = Flask(__name__) # flask application here
@app.route('/initdb')
def create_db():
    DATABASE = init_db(engine)
    make_table(engine)
    with engine.connect() as conn:
        conn.execute(f"USE {DATABASE};")
        TABLES = list(conn.execute('show tables;'))
        return f"successfully made database {DATABASE} with tables {tuple(map(lambda x: x[0], TABLES))}"

@app.route('/initdb/dump')
def dump_database():
    dump_db(engine)
    return '''successfully dumped db'''

@app.route('/user/<name>')
def all_data(name):
    with engine.connect() as conn:  # наименования всех параметров
        all_values = list(conn.execute(select(signatures_table).where(signatures_table.c.name == name)).first())[2:]
        num_col = len(all_columns)
        print(all_columns)
    return render_template('index.html', name=name, all_columns=all_columns, all_values=all_values, num_col=num_col)

@app.route('/user/<name>/<param>')
def single_param(name, param):
    if param.lower() not in all_columns:
        return f"<p>No such param {param}</p>"
    else:
        with engine.connect() as conn:
            value = conn.execute(select(text(f"signatures.{param}")).where(signatures_table.c.name == name)).first()[0]
        return render_template('index_indiv.html', name=name, param=param, value=value)
# allow both GET and POST requests
@app.route('/filter', methods=['GET', 'POST'])
def filter():
    # handle the POST request
    if request.method == 'POST':
        parameter = request.form.get('parameter')
        try:
            max_val = Decimal(request.form.get('max_val'))
        except decimal.InvalidOperation:
            max_val = None
        try:
            min_val = Decimal(request.form.get('min_val'))
        except decimal.InvalidOperation:
            min_val = None
        try:
            return jsonify(
                find_patient(
                    engine, parameter, max_val, min_val
                )
            )
        except TypeError:
            return"<h2>Wrong input</h2>"
    # handle the GET request
    return render_template('simple_search.html')

@app.route('/describe', methods=['GET', 'POST'])
def describe():
    if request.method == 'POST':
        users = request.form.get('users')
        parameter = request.form.get('parameter')
        return jsonify(
            make_describe(
                engine, parameter, users
            )
        )
    return render_template('describe_users.html')
# NSCLC1079, NSCLC1619