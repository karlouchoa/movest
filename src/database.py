from sqlalchemy import create_engine, event

def get_engine(db_name):
    conn_str = (
        f"mssql+pyodbc://localhost/{db_name}?"
        "driver=ODBC+Driver+17+for+SQL+Server&"
        "trusted_connection=yes"
    )
    engine = create_engine(conn_str)
    
    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        if executemany:
            cursor.fast_executemany = True
    return engine

engine_base = get_engine("Bancobase")
engine_atual = get_engine("Bancoatual")