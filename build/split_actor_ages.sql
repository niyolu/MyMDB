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