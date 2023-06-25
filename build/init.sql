GO
CREATE DATABASE MyMDB;
GO


USE MyMDB;

GO

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
    age int NOT NULL,
    avg_rating DECIMAL(3, 1) 
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
GO

CREATE FUNCTION split_actor_ages (
  @actor_ages VARCHAR(MAX)
)
RETURNS TABLE
AS
RETURN
(
  SELECT ordinal as idx, value AS age
  FROM STRING_SPLIT(@actor_ages, ',', 1)
);

GO

CREATE FUNCTION get_actors_for_movie
  (@movie_id INT)
RETURNS VARCHAR(MAX)
AS
BEGIN
  DECLARE @actor_list VARCHAR(MAX);
  SELECT @actor_list = STRING_AGG(name, ', ')
  FROM actors
  JOIN movie_actors ON actors.id = movie_actors.actor_id
  WHERE movie_actors.movie_id = @movie_id;
  RETURN @actor_list;
END;
GO

CREATE PROCEDURE insert_movie
  @title VARCHAR(50),
  @year INT,
  @genre VARCHAR(50),
  @actor_names VARCHAR(MAX),
  @actor_ages VARCHAR(MAX),
  @rating DECIMAL(3, 1)
AS
BEGIN
  DECLARE @movie_id INT;
  IF NOT EXISTS (SELECT * FROM movies WHERE title = @title AND year = @year)
  BEGIN
    INSERT INTO movies (title, year, genre, rating)
    VALUES (@title, @year, @genre, @rating);
    SELECT @movie_id = SCOPE_IDENTITY();
    DECLARE @actor_id INT;
    DECLARE @actor_name VARCHAR(50);
    DECLARE @actor_age INT;
    DECLARE actor_cursor CURSOR FOR
      SELECT value
      FROM STRING_SPLIT(@actor_names, ',');
    DECLARE @actor_ages_table TABLE (
      row_idx INT PRIMARY KEY,
      age INT
    );
    INSERT INTO @actor_ages_table (row_idx, age)
    SELECT idx, age
    FROM split_actor_ages(@actor_ages);
    OPEN actor_cursor;
    FETCH NEXT FROM actor_cursor INTO @actor_name;

    WHILE @@FETCH_STATUS = 0
    BEGIN
      SELECT @actor_age = age FROM @actor_ages_table WHERE row_idx = @@CURSOR_ROWS;
      
      IF NOT EXISTS (SELECT * FROM actors WHERE name = @actor_name AND age = @actor_age)
      BEGIN
        INSERT INTO actors (name, age)
        VALUES (@actor_name, @actor_age);
        SELECT @actor_id = SCOPE_IDENTITY();
      END
      ELSE
      BEGIN
        SELECT @actor_id = id FROM actors WHERE name = @actor_name AND age = @actor_age;
      END

      INSERT INTO movie_actors (movie_id, actor_id)
      VALUES (@movie_id, @actor_id);

      FETCH NEXT FROM actor_cursor INTO @actor_name;
    END

    CLOSE actor_cursor;
    DEALLOCATE actor_cursor;
  END;
END;
GO

CREATE TRIGGER trg_update_avg_rating
ON movie_actors
AFTER INSERT
AS
BEGIN
  DECLARE @actor_id INT;
  SELECT @actor_id = actor_id
  FROM inserted;

  UPDATE actors
  SET avg_rating = (
    SELECT AVG(m.rating)
    FROM movie_actors ma
    JOIN movies m ON ma.movie_id = m.id
    WHERE ma.actor_id = @actor_id
  )
  WHERE id = @actor_id;
END;