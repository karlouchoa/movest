from sqlalchemy import create_engine, event
from sqlalchemy.engine import URL


def get_engine(server_name, db_name, username=None, password=None):
    if username is not None and password is not None and username.strip() != "":
        auth_string = f"UID={username.strip()};PWD={password};"
    else:
        auth_string = "Trusted_Connection=yes;"

    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server_name};"
        f"DATABASE={db_name};"
        f"{auth_string}"
        "TrustServerCertificate=yes;"
    )
    engine = create_engine(URL.create("mssql+pyodbc", query={"odbc_connect": conn_str}))
    
    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        if executemany:
            cursor.fast_executemany = True
    return engine
