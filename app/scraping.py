import asyncio
import aiohttp
import bs4 as bs
from urllib.parse import urljoin
from datetime import date
import time
import typing
import collections

MAX_ACTORS = 100 # FIXME maybe like 20
MAX_MOVIES = 250

url_base = "https://www.imdb.com/"
url_charts = urljoin(url_base, "chart/top")

headers = {
    "user-agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "accept-language": "de-de"
}


def get_month_from_ger_str(m_str: str):
    return 1 + "Januar,Februar,März,April,Mai,Juni,Juli,August,September,Oktober,November,Dezember".split(",").index(m_str)


async def get_top_movies(session: aiohttp.client.ClientSession) -> bs.element.ResultSet:
    async with session.get(url_charts) as response:
        soup = bs.BeautifulSoup(await response.text(), "html.parser")
        movies = soup.select('td.titleColumn')
        return movies


async def parse_movie(
        session: aiohttp.client.ClientSession, 
        movie: bs.element.Tag
    ) -> dict:
    title = movie.find("a").text
    year = int(movie.find("span", {"class": "secondaryInfo"}).text.strip("()"))
    rating = float(movie.find_next_sibling().find("strong").text.replace(",", "."))
    movie_url = urljoin(url_base, movie.find("a")["href"])

    async with session.get(movie_url) as movie_response:
        soup = bs.BeautifulSoup(await movie_response.text(), "html.parser")
        genre = soup.find("a", class_="ipc-chip ipc-chip--on-baseAlt").text
        actors = soup.find_all("a", {"data-testid": "title-cast-item__actor"})
        actor_names, actor_ages = await parse_actors(session, actors)

    return {
        "title": title,
        "year": year,
        "genre": genre,
        "actor_names": actor_names,
        "actor_ages": actor_ages,
        "rating": rating
    }

# cache -> split up in parse actor
async def parse_actors(
        session: aiohttp.client.ClientSession, actors
    ) -> tuple[list[str], list[int]]:
    names, ages = [], []
    for actor in actors:
        names.append(actor.text)
        actor_url = urljoin(url_base, actor["href"])

        async with session.get(actor_url) as actor_response:
            soup = bs.BeautifulSoup(await actor_response.text(), "html.parser")
            try:
                date_of_birth = soup.find("li", {"data-testid": "nm_pd_bl"}).find("a").text
                d, m, y = date_of_birth.split()
                d = int(d.strip("."))
                y = int(y)
                m = get_month_from_ger_str(m)
            except (AttributeError, ValueError) as e:
                print("actor without (correctly) specified date of birth or bio: ", names[-1], actor_url, e)
                age = -1
            else:
                age = (date.today() - date(y, m, d)).days // 365
            ages.append(age)
    return names, ages


async def parse_top_movies(n: int = MAX_MOVIES) -> list:
    movies_info = []
    async with aiohttp.ClientSession(headers=headers) as session:
        movies = await get_top_movies(session)
        tasks = []
        for idx, movie in enumerate(movies):
            if n == idx:
                break
            tasks.append(parse_movie(session, movie))
        movies_info = await asyncio.gather(*tasks)
    return movies_info


def run_scraper(
        scraper: typing.Callable[..., typing.Coroutine[typing.Any, typing.Any, typing.Any]],
        *args, **kwargs
    ) -> list:
    loop = asyncio.get_event_loop()
    movies = loop.run_until_complete(scraper(*args, **kwargs))
    return movies
    
    
def test():
    import sys
    import pickle
    n = sys.argv[1] if len(sys.argv) > 1 else 3#250
    print(n)
    start = time.perf_counter()
    movies = run_scraper(parse_top_movies, n)
    print(movies)
    print(f"script took {time.perf_counter()-start:.2f}s to run")
    with open(f"top{n}.pkl", "wb") as f: 
        pickle.dump(movies, f)
    print(len(movies))
    


if __name__ == "__main__":
    test()

