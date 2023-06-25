FROM mcr.microsoft.com/mssql/server:2022-latest

ENV SA_PASSWORD=""
ENV ACCEPT_EULA=""
ENV MSSQL_PID Express


WORKDIR /usr/src/app

COPY build/ .

# RUN chmod +x /usr/src/app/run-initialization.sh
# RUN chmod +x ./init.sql

CMD /bin/bash ./entrypoint.sh