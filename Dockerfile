FROM mysql/mysql-server:5.7

# Copy the database schema to the /data directory
COPY ./init_schema.sql /data/init_schema.sql

# Change the working directory
WORKDIR data

CMD mysql -u $MYSQL_USER -p $MYSQL_PASSWORD $MYSQL_DATABASE < init_schema.sql