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
      SELECT value, ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
      FROM STRING_SPLIT(@actor_names, ',');
    DECLARE @actor_ages_table TABLE (
      row_idx INT PRIMARY KEY,
      age INT
    );

    INSERT INTO @actor_ages_table
    SELECT idx, age
    FROM SplitActorAges(@actor_ages);

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
