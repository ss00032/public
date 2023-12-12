#!/bin/sh

cd $TS_HOME/bin

TS_CNT\u003d`ps -ef | grep tslmgrd | grep -v grep | wc -l`

if [ $TS_CNT -ge 1 ]; then
  echo \"TeraStream already started\"
else
   rm *sock* 2\u003e\u00261

   tsadmin start
fi