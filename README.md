# MyMDB

Scraper, DB and analysis-notebook for interacting with [IMDb](https://www.imdb.com/). I read and respected the robots.txt I swear.

### Installation

- build docker image for DB `docker build --build-arg ACCEPT_EULA=Y -t my-sql-server . `
- python deps `pip install -r requirements.txt`

### Usage

- Run the DB container via `docker run -e "SA_PASSWORD=<password>" -p 1433:1433 my-sql-server`
- Stop the DB via `docker stop $(docker ps -a -q)` (this will nuke all running containers on your engine)
- To connect, find IP via `docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' my-sql-server`