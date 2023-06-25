CREATE TABLE movies (
  id INT IDENTITY(1,1)  PRIMARY KEY,
  title VARCHAR(50) NOT NULL,
  year INT NOT NULL,
  genre VARCHAR(50) NOT NULL,
  rating DECIMAL(3, 1) NOT NULL
);


CREATE TABLE actors (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    age int NOT NULL
);


CREATE TABLE actor_deleted_ratings (
    actor_id INT,
    rating DECIMAL(3, 1) NOT NULL
);


CREATE TABLE movie_actors (
  movie_id INT,
  actor_id INT,
  FOREIGN KEY (movie_id) REFERENCES movies(id),
  FOREIGN KEY (actor_id) REFERENCES actors(id)
);