import pyodbc

# for driver in pyodbc.drivers():
#     print(driver)

# driver = "ODBC Driver 17 for SQL Server"
driver = "SQL Server"
server = "localhost"
# server = "127.0.0.1"
#server = "localhost\sqlexpress"
port = "1433"
database = "ExampleDB"
username = "SA"
password = "Pr0dRdyPw!"

conn_str = (
    fr"DRIVER={driver};"
    fr"SERVER={server};"
    fr"PORT={port};"
    #fr"DATABASE={database};"
    fr"UID={username};"
    fr"PWD={password}"
)

print(f"connecting to db via conn_str: {conn_str}")

cnxn = pyodbc.connect(conn_str)
cursor = cnxn.cursor()

def 