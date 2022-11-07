from sqlalchemy import text, exc
# from dump_db import make_engine

# with open('../password.txt', 'rt') as f:
#         username, host, database, password = map(lambda x: x.split()[0], f.readlines())
#         engine = make_engine(username=username, host=host, password=password)
#

columns = ['mhci', 'mhcii', 'coactivation_molecules', 'effector_cells', 't_cell_traffic', 'nk_cells',
               't_cells', 'b_cells', 'm1_signatures', 'th1_signature', 'antitumor_cytokines', 'checkpoint_inhibition',
               'treg', 't_reg_traffic', 'neutrophil_signature', 'granulocyte_traffic', 'mdsc', 'mdsc_traffic',
               'macrophages', 'macrophage_dc_traffic', 'th2_signature', 'protumor_cytokines', 'caf', 'matrix',
           'matrix_remodeling', 'angiogenesis', 'endothelium', 'proliferation_rate', 'emt_signature']

def find_patient(engine, param : str, max_threshold=None, min_threshold=None, columns = columns): # тестируем поиск на минималках

    result = None

    if param.lower() not in columns:
        return result

    with engine.connect() as conn:
        if min_threshold is not None and max_threshold is not None:
            if min_threshold > max_threshold:
                min_threshold, max_threshold = max_threshold, min_threshold
            result = conn.execute(
                text(
                    f"select id, name, {param} from `signatures` where {param} >= {min_threshold} and {param} <= {max_threshold};"
                )
            )
        elif min_threshold is not None:
            result = conn.execute(
                text(
                    f"select id, name, {param} from `signatures` where {param} >= {min_threshold};"
                )
            )
        elif max_threshold is not None:
            result = conn.execute(
                text(
                    f"select id, name, {param} from `signatures` where {param} <= {max_threshold};"
                )
            )
        if result:
            return list(
                map(
                    lambda x: {'id' : x[0], 'name' : x[1], f'{param}' : str(x[2])}, result
                )
            )
        else:
            return None

def make_describe(engine, param, users, columns = columns):

    result = None
    if param.lower() not in columns:
        return f"No such param '{param}'"

    with engine.connect() as conn:
        users_list = tuple(
            map(
                lambda x: str(x.strip()), users.split(',')
            )
        )
        try:
            result = conn.execute(
                text(
                    f"select `id`, `name`, `{param}` from `signatures` where `name` in {users_list};"
                )
            )
        except exc.ProgrammingError:
            return f"incorrect user input '{users}'"
    return list(
                map(
                    lambda x: {'id' : x[0], 'name' : x[1], f'{param}' : str(x[2])}, result
                )
            )

#     signature_columns = list(map(lambda x : x[0], conn.execute(text(f"select column_name from information_schema.columns\
#         where table_schema = '{database}'\
#         order by ordinal_position "))))[2:]
#     print(signature_columns)