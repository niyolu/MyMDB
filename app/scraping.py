import asyncio
import aiohttp
from aiocache import cached

import bs4 as bs
from urllib.parse import urljoin

from datetime import date
import time
import typing
from functools import partial
import random


MAX_ACTORS = 20 # FIXME maybe like 20
MAX_MOVIES = 250
SCRAPE_DELAY = (10, 15)

URL_BASE = "https://www.imdb.com/"
URL_CHARTS = urljoin(URL_BASE, "chart/top")
URL_ROULETTE = urljoin(URL_BASE, "list/ls091294718/")

headers = {
    # "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
    "user-agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "accept-language": "de-de"
}

async def aggregate_safely(tasks):
    container = []
    for coroutine in asyncio.as_completed(tasks):
        try:
            results = await coroutine
        except Exception as e:
            print('Got an exception:', e)
        else:
            container.append(results)
        # container.append(await coroutine)
    return container


def get_month_from_ger_str(m_str: str):
    return 1 + "Januar,Februar,März,April,Mai,Juni,Juli,August,September,Oktober,November,Dezember".split(",").index(m_str)


async def get_top_movies(
        session: aiohttp.client.ClientSession,
        n: int = MAX_MOVIES
    ) -> bs.element.ResultSet:
    async with session.get(URL_CHARTS) as response:
        soup = bs.BeautifulSoup(await response.text(), "html.parser")
        movies = soup.select('td.titleColumn')
        return movies[:n]
    
async def get_random_movies(
        session: aiohttp.client.ClientSession,
        n: int = MAX_MOVIES
    ) -> bs.element.ResultSet:
    async with session.get(URL_ROULETTE) as response:
        soup = bs.BeautifulSoup(await response.text(), "html.parser")
        movies = soup.select('td.titleColumn')
        return movies[:n]


async def parse_movie(
        session: aiohttp.client.ClientSession, 
        movie: bs.element.Tag
    ) -> dict:
    title = movie.find("a").text
    year = int(movie.find("span", {"class": "secondaryInfo"}).text.strip("()"))
    rating = float(movie.find_next_sibling().find("strong").text.replace(",", "."))
    movie_url = urljoin(URL_BASE, movie.find("a")["href"])

    #async with session.get(movie_url, timeout=aiohttp.ClientTimeout(total=60)) as movie_response:
    async with session.get(movie_url) as movie_response:
        soup = bs.BeautifulSoup(await movie_response.text(), "html.parser")
        genre = soup.find("a", class_="ipc-chip ipc-chip--on-baseAlt").text
        actors = soup.find_all("a", {"data-testid": "title-cast-item__actor"})
        try:
            budget = int(
                soup
                .find("li", {"data-testid": "title-boxoffice-budget"})
                .find("li")
                .text.split()[0].replace(".", "")
            )
        except Exception as e:
            print(e)
            print(f"happened with budget in {title}: {movie_url}")
            budget = None
        try:
            gross_income = int(
                soup
                .find("li", {"data-testid": "title-boxoffice-cumulativeworldwidegross"})
                .find("li")
                .text.split()[0].replace(".", "")
            )
        except Exception as e:
            print(e)
            print(f"happened with gross_income in {title}: {movie_url}")
            gross_income = None
        actor_names, actor_ages = await parse_actors(session, actors)

    return {
        "title": title,
        "year": year,
        "genre": genre,
        "actor_names": actor_names,
        "actor_ages": actor_ages,
        "rating": rating,
        "budget": budget,
        "gross_income": gross_income,
    }

# FIXME proably source of dupes
#@cached(key_builder=lambda *args, **kwargs: kwargs["actor"].text)
async def parse_actor(
        *,
        session: aiohttp.client.ClientSession,
        actor: bs.element.Tag
    ) -> tuple:
    time.sleep(random.random() * 2 if random.random()<0.5 else random.random() * 10)
    name = actor.text
    actor_url = urljoin(URL_BASE, actor["href"])
    #async with session.get(actor_url, timeout=aiohttp.ClientTimeout(total=60)) as actor_response:
    async with session.get(actor_url) as actor_response:
        soup = bs.BeautifulSoup(await actor_response.text(), "html.parser")
        try:
            date_of_birth = soup.find("li", {"data-testid": "nm_pd_bl"}).find("a").text
            d, m, y = date_of_birth.split()
            d = int(d.strip("."))
            y = int(y)
            m = get_month_from_ger_str(m)
        except (AttributeError, ValueError) as e:
            print("actor without (correctly) specified date of birth or bio: ", name, actor_url, e)
            age = -1
        else:
            age = (date.today() - date(y, m, d)).days // 365
    return name, age


async def parse_actors(
        session: aiohttp.client.ClientSession,
        actors: bs.element.ResultSet
    ) -> tuple[list[str], list[int]]:
    tasks = []
    parse = partial(parse_actor, session=session)
    tasks = [parse(actor=actor) for actor in actors[:MAX_ACTORS]]
    names_ages = await aggregate_safely(tasks)
    return zip(*names_ages)


async def parse_top_movies(n: int = MAX_MOVIES) -> list:
    async with aiohttp.ClientSession(headers=headers) as session:
        movies = await get_top_movies(session, n)
        if not movies:
            return []
        parse = partial(parse_movie, session=session)
        tasks = [parse(movie=movie) for movie in movies]
        return await aggregate_safely(tasks)


def run_scraper(
        scraper: typing.Callable[..., typing.Coroutine[typing.Any, typing.Any, typing.Any]],
        *args, **kwargs
    ) -> list:
    loop = asyncio.get_event_loop()
    movies = loop.run_until_complete(scraper(*args, **kwargs))
    return movies


def scrape_top_movies(n: int = MAX_MOVIES) -> list:
    return run_scraper(parse_top_movies, n)
    
    
def test():
    import sys
    import pickle
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 3#250
    start = time.perf_counter()
    movies = run_scraper(parse_top_movies, n)
    print(movies)
    print(f"script took {time.perf_counter()-start:.2f}s to parse {len(movies)} movies")
    if movies:
        if len(movies) == n or input(f"fetched {len(movies)}/{n} movies, continue? y/n\n").lower() == "y":
            with open(f"top{n}.pkl", "wb") as f: 
                pickle.dump(movies, f)


if __name__ == "__main__":
    test()
    