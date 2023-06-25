# Wait to be sure that SQL Server came up
sleep 10s

/opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P $SA_PASSWORD -d master -i init.sql