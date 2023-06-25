import pyodbc

for driver in pyodbc.drivers():
    print(driver)

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

# # print("version:")
# # cursor.execute("SELECT @@version;") 
# # row = cursor.fetchone() 
# # while row: 
# #     print(row[0])
# #     row = cursor.fetchone()
# cursor.execute("SELECT * FROM STRING_SPLIT('A.B.C', '.', 1)")
# for row in cursor.fetchall():
#     print(row)

    

# Execute a SQL query
cursor.execute("SELECT * FROM actors")

# Fetch the results
results = cursor.fetchall()

print("accessing actors")
# Do something with the results
for row in results:
    print(row)
    
cursor.execute("{call insert_movie(1,2,3)}")

# Close the cursor and connection
cursor.close()
cnxn.close() 