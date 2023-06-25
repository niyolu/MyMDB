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