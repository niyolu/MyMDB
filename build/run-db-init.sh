# Wait to be sure that SQL Server came up
sleep 10s

# Run the setup script to create the DB and the schema in the DB
# Note: make sure that your password matches what is in the Dockerfile
/opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P $SA_PASSWORD -d master \
    -i tables.sql \
    -i split_actor_ages.sql \
    -i get_actors_for_movie.sql \
    -i insert_movie.sql