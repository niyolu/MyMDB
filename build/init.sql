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
  avg_rating DECIMAL(3, 1) DEFAULT 0
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
  SELECT ordinal as idx, CAST(value as int) AS age
  FROM STRING_SPLIT(@actor_ages, ',', 1)
);

GO

CREATE FUNCTION split_actor_names (
  @actor_names VARCHAR(MAX)
)
RETURNS TABLE
AS
RETURN
(
  SELECT ordinal as idx, value AS name
  FROM STRING_SPLIT(@actor_names, ',', 1)
);
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
    print 'inserting movie'
    INSERT INTO movies (title, year, genre, rating)
    VALUES (@title, @year, @genre, @rating);
    SELECT @movie_id = SCOPE_IDENTITY();
   -- DECLARE @actor_id INT;
    DECLARE @actor_name VARCHAR(50);
    DECLARE @actor_age INT;

    DECLARE @actor_ages_table TABLE (
      row_idx INT PRIMARY KEY,
      age INT
    );

    INSERT INTO @actor_ages_table (row_idx, age)
    SELECT idx, age
    FROM split_actor_ages(@actor_ages);

    DECLARE @actor_names_table TABLE (
      row_idx INT PRIMARY KEY,
      name VARCHAR(MAX)
    );
    INSERT INTO @actor_names_table (row_idx, name)
    SELECT idx, name
    FROM split_actor_names(@actor_names);

    DECLARE actor_cursor CURSOR FOR
      SELECT name, age
      FROM @actor_ages_table ag
      JOIN @actor_names_table an ON ag.row_idx = an.row_idx;

    OPEN actor_cursor;
    FETCH NEXT FROM actor_cursor INTO @actor_name, @actor_age;

    DECLARE @i INT = 0;

    -- try to insert into actors or change a broken old record
    -- actors have the tendency to avoid choosing another ones name but if that
    -- happens below logic allows for two or more actors where one had a badly
    -- scraped age now share their associated movies.
    WHILE @@FETCH_STATUS = 0
    BEGIN
      print 'i'
      print @i;

      SET @i = @i + 1;
      DECLARE @actor_id INT = NULL;
      DECLARE @previous_actor_age INT;
      SELECT @actor_id = id, @previous_actor_age = age from actors WHERE name = @actor_name;

      -- abuse int as bool, valid means age
      DECLARE @this_valid INT = IIF(@actor_age <> -1, 1, 0);
      DECLARE @previous_valid INT = IIF(@previous_actor_age <> -1, 1, 0);
      DECLARE @previous_exists INT = IIF(@actor_id IS NOT NULL, 1, 0);
      DECLARE @real_age_mismatch INT =
        IIF(@this_valid=1 AND @previous_valid=1 AND @actor_age <> @previous_actor_age, 1, 0);

      print 'age,name,prevage'
      print @actor_age
      print @actor_name
      print @previous_actor_age

      print 'declared cases: this_valid previous_valid previous_exists real_age_mismatch'
      print @this_valid
      print @previous_valid
      print @previous_exists
      print @real_age_mismatch

      IF @previous_exists=0 OR @real_age_mismatch=1
      BEGIN
        print '@previous_exists=0 OR @real_age_mismatch=1 INSERT ACTOR'
        INSERT INTO actors (name, age)
          VALUES (@actor_name, @actor_age);
        SELECT @actor_id = SCOPE_IDENTITY();
      END
      ELSE IF @this_valid = 1 AND @previous_valid = 0
      BEGIN
        print 'IF @this_valid=1 AND @previous_valid=0 UPDATE ACTOR'
        UPDATE actors
          SET age = @actor_age
          WHERE id = @actor_id;
      END

      IF @actor_id IS NOT NULL
      BEGIN
          print 'insert movie actors'
        INSERT INTO movie_actors (movie_id, actor_id)

          VALUES (@movie_id, @actor_id);
      END

      FETCH NEXT FROM actor_cursor INTO @actor_name, @actor_age;
    END

    CLOSE actor_cursor;
    DEALLOCATE actor_cursor;
  END
END;
GO

CREATE FUNCTION recursive_avg (
  @avg_km1 DECIMAL(3,1),
  @k INT,
  @x_k DECIMAL(3,1)
)
RETURNS DECIMAL(3,1)
AS
BEGIN
  DECLARE @avg_k DECIMAL(3,1)
  SET @avg_k = @avg_km1 + (@x_k - @avg_km1) / @k;
  RETURN(@avg_k)
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

  DECLARE @movie_id INT;
  SELECT @movie_id = movie_id
  FROM inserted;

  DECLARE @prev_avg_rating DECIMAL(3,1);
  SELECT @prev_avg_rating = avg_rating
    FROM actors WHERE id = @actor_id;

  DECLARE @num_appearances INT;
  SELECT @num_appearances = COUNT (*) FROM movie_actors
    WHERE actor_id = @actor_id;

  DECLARE @new_rating DECIMAL(3,1);
  SELECT @new_rating = rating
    FROM movies WHERE id = @movie_id;

  DECLARE @avg_rating DECIMAL(3,1);
  SET @avg_rating = dbo.recursive_avg(
    @prev_avg_rating, @num_appearances, @new_rating
  );

  UPDATE actors
    SET avg_rating = (
      @avg_rating
    )
    WHERE id = @actor_id;
END;
GO

-- CREATE FUNCTION get_actors_for_movie
--   (@movie_id INT)
-- RETURNS VARCHAR(MAX)
-- AS
-- BEGIN
--   DECLARE @actor_list VARCHAR(MAX);
--   SELECT @actor_list = STRING_AGG(name, ', ')
--   FROM actors
--   JOIN movie_actors ON actors.id = movie_actors.actor_id
--   WHERE movie_actors.movie_id = @movie_id;
--   RETURN @actor_list;
-- END;
-- GO



-- CREATE TRIGGER trg_update_avg_rating
-- ON movie_actors
-- AFTER INSERT
-- AS
-- BEGIN
--   DECLARE @actor_id INT;
--   SELECT @actor_id = actor_id
--   FROM inserted;

--   UPDATE actors
--   SET avg_rating = (
--     SELECT AVG(m.rating)
--     FROM movie_actors ma
--     JOIN movies m ON ma.movie_id = m.id
--     WHERE ma.actor_id = @actor_id
--   )
--   WHERE id = @actor_id;
-- END;