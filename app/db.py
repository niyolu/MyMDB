import pyodbc
from typing import List

# for driver in pyodbc.drivers():
#     print(driver)

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
        rating: int,
        budget: int = None,
        gross_income: int = None
    ):
    def convert(x):
        if isinstance(x, str):
            return repr(x.replace("'", "§")).replace("§", "''")
        if isinstance(x, (int, float)):
            return str(x)
        if isinstance(x, (tuple, list)):
            return convert(",".join(map(str, x)))    
        raise NotImplementedError(f"no impl for type {type(x)}")
    
    args = ",".join([
        convert(arg) for arg in (
                title,
                year,
                genre,
                actor_names,
                actor_ages,
                rating,
                budget,
                gross_income
        )
        if arg is not None
    ])
    call_str = f"{{call insert_movie({args})}}"
    # print(call_str)
    res = cursor.execute(call_str)
    cnxn.commit()
    return res
    
    
def select_all(tablename: str):
    cursor.execute(f"SELECT * from {tablename}")
    results = cursor.fetchall()
    return results
        
        
def print_all_tables():
    for tbl in ("actors", "movies", "movie_actors"):
        print("printing table", tbl)
        print(select_all(tbl))
        
        
def get_actor_ranking(top=None):
    cursor.execute("SELECT name, avg_rating from actors ORDER BY avg_rating DESC")
    return cursor.fetchall()[:top] if top else cursor.fetchall()


def get_actor_appearances():
    cursor.execute("SELECT name, COUNT(*) from actors a join movie_actors ma on a.id = ma.actor_id GROUP BY name ORDER BY COUNT(*) DESC, name DESC")
    return cursor.fetchall()


def get_actor_appearances_ratings_age():
    cursor.execute("SELECT name, age, COUNT(*) AS appearances, avg_rating FROM actors a JOIN movie_actors ma ON a.id = ma.actor_id WHERE age <> -1 GROUP BY name, age, avg_rating ORDER BY avg_rating DESC, COUNT(*) DESC, name DESC;")
    return cursor.fetchall()


def get_movie_yr_budget_income_rating():
    cursor.execute("SELECT year, budget, gross_income, rating FROM movies")
    return cursor.fetchall()


def check_no_duplicates() -> bool:
    cursor.execute("SELECT name, COUNT(*) from actors GROUP BY NAME HAVING COUNT(*) > 1")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    return not rows

        
def test_harcoded():
    names = [
        'Tim Robbins', "Morgan d'Freeman", 'Bob Gunton', 'William Sadler', 'Clancy Brown',
        'Gil Bellows', 'Mark Rolston', 'James Whitmore', 'Jeffrey DeMunn', 'Larry Brandenburg',
        'Neil Giuntoli', 'Brian Libby', 'David Proval', 'Joseph Ragno', 'Jude Ciccolella',
        'Paul McCrane', 'Renee Blaine', 'Scott Mann'
    ]
    insert_movie(
        title="Die Verurteilten", year=1994, genre="Drama", rating=9.2,
        actor_ages=[30+idx for idx, x in enumerate(names)], actor_names=names,
        #budget=25000000, gross_income=28884504
    )
    

    print_all_tables()
    
    names = [
        'Tim Robbins', 'Morgan Freeman', 'Bob Gunton', 'William Sadler', 'Clancy Brown',
        'Gil Bellows', 'Mark Rolston', 'James Whitmore', 'Jeffrey DeMunn', 'Larry Brandenburg',
        'Neil Giuntoli', 'Brian Libby', 'David Proval', 'Joseph Ragno', 'Jude Ciccolella',
        'Paul McCrane', 'Renee Blaine', 'Scott Mann', "John Doe"
    ]
    insert_movie(
        title="Die Verurteilten2", year=1995, genre="Dramacomedy", rating=1.3,
        actor_ages=[30+idx for idx, x in enumerate(names)], actor_names=names,
        budget=25000000, gross_income=28884504
    )
    print_all_tables()
    assert check_no_duplicates()
    
def test_pickled():
    import pickle

    # with open("./moviedata/top100.pkl", "rb") as f:
    #     movies = pickle.load(f)
    # print(f"inserting {len(movies)}")
    # for movie in movies:
    #     try:
    #         insert_movie(**movie)
    #     except:
    #         print("issue at", movie)
            
    print_all_tables()
    assert check_no_duplicates()


if __name__ == "__main__":
    test_harcoded()
    # test_pickled()