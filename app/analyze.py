# plot number of occurences as barplot
# plot distribution of average ratings with suitable bin
# plot average rating per year
# maybe some ML on (age, avg_rating) -> occurences
# scatterplot matrix


import pandas as pd

def get_movie_dataframe(movies):
    """ (61, 'Die Verurteilten', 1994, 'Drama', Decimal('9.2') """
    return pd.DataFrame(
            movies, columns=["index", "title", "year", "genre", "rating"],
    ).set_index(["title", "year"])# .drop(columns=["index"])
    
    
def get_actors_dataframe(actors):
    """ (1015, 'Tim Robbins', 64, Decimal('9.2')) """
    return pd.DataFrame(
        actors, columns=["index", "name", "age", "avg_rating"]
    ).set_index(["name", "age"]) #.drop(columns=["index"])
    

def get_movie_actors_dataframe(move_actors):
    return pd.DataFrame(
        move_actors, columns = ["actor_id", "movie_id"]
    ).set_index(["actor_id", "movie_id"])