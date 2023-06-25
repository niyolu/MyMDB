import bs4 as bs
import requests
from urllib.parse import urljoin
from datetime import date
import time

MAX_ACTORS = 15
MAX_MOVIES = 20

s = requests.Session()
s.headers.update({
    "user-agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "accept-language": "de-de"
})

url_base = "https://www.imdb.com/"
url_charts = urljoin(url_base, "chart/top")


def get_month_from_ger_str(m_str):
    return 1 + "Januar,Februar,März,April,Mai,Juni,Juli,August,September,Oktober,November,Dezember".split(",").index(m_str)


def get_top_movies() -> bs.element.ResultSet:
    soup = bs.BeautifulSoup(s.get(url_charts).text, "html.parser")
    movies = soup.select('td.titleColumn')
    return movies

def parse_movie(movie: bs.element.Tag):
    title = movie.find("a").text
    year = int(
        movie
        .find("span", {"class": "secondaryInfo"})
        .text.strip("()")
    )
    rating = float(
        movie
        .find_next_sibling()
        .find("strong")
        .text.replace(",", ".")
    )
    movie_url = urljoin(url_base, movie.find("a")["href"])
    movie_response = s.get(movie_url)
    soup = bs.BeautifulSoup(movie_response.text, "html.parser")
    genre = soup.find("a", class_="ipc-chip ipc-chip--on-baseAlt").text
    actors = soup.find_all("a", {"data-testid": "title-cast-item__actor"})
    actor_names, actor_ages = parse_actors(actors)
    return {
        "title": title,
        "year": year,
        "genre": genre,
        "actor_names": actor_names,
        "actor_ages": actor_ages,
        "rating": rating
    }
    

def parse_actors(actors: bs.element.ResultSet):
    names, ages = [], []
    for actor in actors:
        names.append(actor.text)
        actor_url = urljoin(url_base, actor["href"])
        soup = bs.BeautifulSoup(s.get(actor_url).text, "html.parser")
        try:
            year_of_birth = soup.find("li", {"data-testid":"nm_pd_bl"}).find("a").text
            d,m,y = year_of_birth.split()
            d = int(d.strip("."))
            y = int(y)
            m = get_month_from_ger_str(m)
        except Exception as e:
            ages.append(-1)
            print(e, names[-1], actor_url)
            continue
        age = (date.today() - date(y, m, d)).days // 365
        ages.append(age)
    return names, ages
        
    
def parse_top_movies(n: int = MAX_MOVIES) -> list[dict]:
    movies_info = []
    movies = get_top_movies()
    for idx, movie in enumerate(movies):
        if n == idx:
            break
        time.sleep(.5)
        movies_info.append(parse_movie(movie))
    return movies_info

def test():
    print(parse_top_movies(10))
    
        
if __name__ == "__main__":
    start = time.perf_counter()
    test()
    print(f"script took {time.perf_counter()-start:.2f}s to run")