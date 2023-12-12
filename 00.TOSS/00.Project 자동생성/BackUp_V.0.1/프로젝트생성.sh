.scm
------
export TS_DBUSER\u003droot
export TS_DBPASS\u003d
export TS_DBPORT\u003d3306
export TS_DBSOCK\u003d/tmp/mysql.sock
export TS_DBHOST\u003d127.0.0.1
export TS_DBNAME\u003dworks50



all.sh
-------
. .scm

echo \"run~\"
find ./$1 -name \u0027*.ts\u0027 -type f -exec sh /home/tstream/.scm/scm.sh {} \\;
#find ./$1 -name \u0027*.ts\u0027 -type f -exec echo {} \\;
echo \"finish~\"



scm.sh
--------
echo $1 | tee -a scm.log
scmdecode works50 $1 | tee -a scm.log