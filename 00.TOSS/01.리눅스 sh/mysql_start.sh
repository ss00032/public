#!/bin/sh

cd $MYSQL_HOME

MYSQL_CNT\u003d`netstat -an | grep LISTEN | grep \":3306\" | grep -v grep | wc -l`

if [ $MYSQL_CNT -ge 1 ]; then
   echo \"MYSQL already started\"
else
   ./bin/mysqld_safe --defaults-file\u003d/engn01/tstream/TeraStream/mysql/data/my.cnf --port\u003d3306 --socket\u003d/tmp/mysql.sock \u0026
fi