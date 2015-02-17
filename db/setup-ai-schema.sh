#!/bin/bash
echo "******CREATING ANALYTICS INBOX DATABASE******"
if [ -z "$POSTGRES_PASSWORD" ]
then
echo "****** POSTGRES_PASSWORD has not been set."
echo "****** POSTGRES_USER=$POSTGRES_USER"
echo "****** These are used to set DB root user / pwd, default values are postgres/<no pwd>"
echo "****** Set them with docker run -e POSTGRES_USER=<user> -e POSTGRES_PASSWORD=<password>"
echo ""
else
echo "****** POSTGRES_PASSWORD has been set."
echo "****** POSTGRES_USER=$POSTGRES_USER"
echo ""
fi

gosu postgres postgres --single -E <<- EOSQL
        CREATE DATABASE ai_production;
        GRANT ALL PRIVILEGES ON DATABASE ai_production to $POSTGRES_USER;
EOSQL
gosu postgres postgres --single -j -E ai_production < /docker-entrypoint-initdb.d/create_user_tables.sql
gosu postgres postgres --single -j -E ai_production < /docker-entrypoint-initdb.d/create_log_tables.sql
gosu postgres postgres --single -j -E ai_production < /docker-entrypoint-initdb.d/add_table_mgmt_columns_to_users.sql

echo ""
echo "******DOCKER ANALYTICS INBOX CREATED******"
echo ""
