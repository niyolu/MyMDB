import pyodbc
from typing import List

# for driver in pyodbc.drivers():
#     print(driver)

# driver = "ODBC Driver 17 for SQL Server"
driver = "SQL Server"
server = "localhost"
# server = "127.0.0.1"
#server = "localhost\sqlexpress"
port = "1433"
database = "MyMDB"
username = "SA"
password = "Pr0dRdyPw!"

conn_str = (
    fr"DRIVER={driver};"
    fr"SERVER={server};"
    fr"PORT={port};"
    fr"DATABASE={database};"
    fr"UID={username};"
    fr"PWD={password}"
)

print(f"connecting to db via conn_str: {conn_str}")

cnxn = pyodbc.connect(conn_str)
cursor = cnxn.cursor()

def insert_movie(
    title: str,
    year: int,
    genre: str,
    actor_names: list[str],
    actor_ages: list[int],
    rating: int
    ):
    def convert(x):
        if isinstance(x, str):
            return repr(x)
        if isinstance(x, (int, float)):
            return str(x)
        if isinstance(x, list):
            return convert(",".join(map(str, x)))
        else:
            print(x, type(x))
            raise NotImplementedError()
    
    args = ",".join([
        convert(arg) for arg in (
                title,
                year,
                genre,
                actor_names,
                actor_ages,
                rating
            )
    ])
    call_str = f"{{call insert_movie({args})}}"
    print(call_str)
    cursor.execute(call_str)
    
def select_all(tablename: str):
    cursor.execute(f"SELECT * from {tablename}")
    results = cursor.fetchall()

    print(f"printing {tablename}")
    for row in results:
        print(row)

def test():
    names = [
        'Tim Robbins', 'Morgan Freeman', 'Bob Gunton', 'William Sadler', 'Clancy Brown',
        'Gil Bellows', 'Mark Rolston', 'James Whitmore', 'Jeffrey DeMunn', 'Larry Brandenburg',
        'Neil Giuntoli', 'Brian Libby', 'David Proval', 'Joseph Ragno', 'Jude Ciccolella',
        'Paul McCrane', 'Renee Blaine', 'Scott Mann'
    ]
    insert_movie(
        title="Die Verurteilten", year=1994, genre="Drama", rating=9.2,
        actor_ages=[30 for x in names], actor_names=names
    )
    [select_all(tbl) for tbl in ("actors", "movies", "movie_actors")]
    
    names = [
        'Tim Robbins', 'Morgan Freeman', 'Bob Gunton', 'William Sadler', 'Clancy Brown',
        'Gil Bellows', 'Mark Rolston', 'James Whitmore', 'Jeffrey DeMunn', 'Larry Brandenburg',
        'Neil Giuntoli', 'Brian Libby', 'David Proval', 'Joseph Ragno', 'Jude Ciccolella',
        'Paul McCrane', 'Renee Blaine', 'Scott Mann', "John Doe"
    ]
    insert_movie(
        title="Die Verurteilten2", year=1995, genre="Dramacomedy", rating=1.3,
        actor_ages=[30 for x in names], actor_names=names
    )
    [select_all(tbl) for tbl in ("actors", "movies", "movie_actors")]

if __name__ == "__main__":
    test()