import pandas as pd
import db
import matplotlib.pyplot as plt


def convert(rows):
    if rows and not isinstance(rows[0], list):
        rows = [list(row) for row in rows]
    return rows


# TODO: rename all of them to create_
def get_movie_dataframe(movies=None):
    """ (61, 'Die Verurteilten', 1994, 'Drama', Decimal('9.2') """
    
    return pd.DataFrame(
            convert(movies or db.select_all("movies")),
            columns=["id", "title", "year", "genre", "rating", "budget", "gross_income"],
    )#.set_index(["index"])# .drop(columns=["index"])
    

# TODO: get_help
def get_actors_dataframe(actors=None):
    """ (1015, 'Tim Robbins', 64, Decimal('9.2')) """
    # actors =)
    # types = None
    # for actor in actors:
    #     new_types = [type(a) for a in actor]
    #     if types:
    #         assert all(t is nt for t, nt in zip(types, new_types))
    #     types = new_types
    
    df = pd.DataFrame(
         convert(actors or db.select_all("actors")),
        columns=["id", "name", "age", "avg_rating"]
    )#.set_index("index") #.drop(columns=["index"])
    df["avg_rating"] = df["avg_rating"].apply(pd.to_numeric)
    # print([(x, type(x)) for x in db.select_all("actors")[0]])
    # print([(x, type(x)) for x in df.iloc[0]])
    # #print(df.dtypes)
    # print(df["age"].dtype)
    # df.describe()
    return df
    

def get_movie_actors_dataframe(move_actors=None):
    return pd.DataFrame(
        convert(move_actors or db.select_all("movie_actors")),
        columns = ["movie_id", "actor_id"]
    )#.set_index(["actor_id", "movie_id"])
    
    
def get_actor_feature_dataframe():
    features = db.get_actor_appearances_ratings_age()
    df = pd.DataFrame(
        convert(features),
        columns = ["name", "age", "appearances", "avg_rating"]
    )
    df["avg_rating"] = df["avg_rating"].apply(pd.to_numeric)
    return df


def get_actor_feature_dataframe():
    features = db.get_actor_appearances_ratings_age()
    df = pd.DataFrame(
        convert(features),
        columns = ["name", "age", "appearances", "avg_rating"]
    )
    df["avg_rating"] = df["avg_rating"].apply(pd.to_numeric)
    return df


def get_movie_feature_dataframe():
    features = db.get_movie_yr_budget_income_rating()
    df = pd.DataFrame(
        convert(features),
        columns = ["year", "budget", "gross_income", "rating"]
    )
    
    df["budget"] = df["budget"].apply(pd.to_numeric)
    df["gross_income"] = df["gross_income"].apply(pd.to_numeric)
    df["rating"] = df["rating"].apply(pd.to_numeric)
    print(df.dtypes)
    return df

    
def get_summary_dataframe(df_actors=None, df_movie_actors=None, df_movies=None):
    return (
        df_actors or get_actors_dataframe()
    ).merge(
        df_movie_actors or get_movie_actors_dataframe(),
        left_on="id", right_on="actor_id", how="inner"
    ).merge(
        df_movies or get_movie_dataframe(),
        left_on="movie_id", right_on="id", how="inner"
    ).drop(["id_x", "id_y"], axis=1)


def scatter_actor_features(df):
    x = df["appearances"]
    y = df["avg_rating"]
    plt.xlabel("appearances")
    plt.ylabel("avg")
    plt.gca().set_ylim([6,10])
    plt.plot(x,y, ".")
    plt.show()
    
def scattermatrix_actors(df):
    pd.plotting.scatter_matrix(df, alpha=0.4, diagonal="kde")
    plt.show()
    

def test_create():
    print(get_movie_feature_dataframe())
    raise
    print(get_actors_dataframe())
    print(get_movie_dataframe())
    print(get_movie_actors_dataframe())
    # movie_actors_df = get_movie_actors_dataframe()
    # print(movie_actors_df)
    # #print(movie_actors_df[movie_actors_df["actor_id"] == 64])
    print(get_summary_dataframe())
    print(get_actor_feature_dataframe())


def test_plot():
    df_actor_features = get_actor_feature_dataframe()
    scatter_actor_features(df_actor_features)
    scattermatrix_actors(df_actor_features)

if __name__ == "__main__":
    test_create()
    test_plot()
    
    

# plot number of occurences as barplot
# plot distribution of average ratings with suitable bin
# plot average rating per year
