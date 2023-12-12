#!/bin/bash

echo \"\"
echo \" \u003c\u003c SOR Data/Log Date Dir Make \u003e\u003e \"
echo \"\"

if [ $# -ne 1 ]
then
           echo \"Invalid Arguments!\"
           echo \"Usage : $ {basename $0) 20201201
           exit 1
fi

JOB_DATE\u003d$1

echo \"Job Date : $JOB_DATE\"
echo \"\"

if [ ${#JOB_DATE} -ne 8 ]
then
           echo \"Job Date Argument Invalid! [$1]\"
           echo \"Usage : $0 20201201\"
           exit 1
fi

if [ \"$JOB_DATE\" !\u003d \"${date +%Y%m%d -d $JOB_DATE 2\u003e\u00261)\" ]
then
           echo \"Job Date Argument Invalid date! [$1]\"
           echo \"Usage : $0 20201201\"
           exit 1
fi

DATA_PATH\u003d\"/data01/tstream/etl\"
LOG_PATH\u003d\"/logs01/tstream/etl\"

SUBS\u003d(\"DOP\" \"DPD\" \"DPL\" \"DPV\" \"DPG\" \"DHC\")

DIR_ARR\u003d()

for SUB in ${SUBS[@]}; do
        DIR_ARR+\u003d(\"${DATA_PATH}/chg/${JOB_DATE}/sam/${SUB}\")
done

DIR_ARR+\u003d(\"${DATA_PATH}/chg/${JOB_DATE}/bad\")
DIR_ARR+\u003d(\"${LOG_PATH}/chg/${JOB_DATE}/log\")

for SUB in ${SUBS[@]}; do
        DIR_ARR+\u003d(\"${DATA_PATH}/ini/${JOB_DATE}/sam/${SUB}\")
done

DIR_ARR+\u003d(\"${DATA_PATH}/ini/${JOB_DATE}/bad\")
DIR_ARR+\u003d(\"${LOG_PATH}/ini/${JOB_DATE}/log\")

for DIR in ${DIR_ARR[@]}; do
       echo \"mkdir : $DIR\"
       mkdir -p $DIR\"
       RTN\u003d$?
       if [ $RTN -ne 0 ]
       then
                 echo \"Make Dir Fail!\"
                 exit 1
        fi
done

echo \"\"
echo \"Make Dir ok!\"