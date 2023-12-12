#!/bin/bash

# Usage : toss_exec.sh [project name] [parameter2]
# parameter1 : project name
# parameter2 : 
# EX) : toss_exec.sh project_test 20201231

TS_DB\u003d`grep \u0027^DB_NAME\u0027 $TS_HOME/bin/tss.ini | awk \u0027BEGIN {FS\u003d\"\u003d\"} {print $2}\u0027`
IP\u003d127.0.0.1
PORT\u003d`grep \u0027^PORT\u0027 $TS_HOME/bin/tss.ini | awk \u0027BEGIN {FS\u003d\"\u003d\"} {print $2}\u0027`

RE_TRY_CNT\u003d6
RE_TRY_SEC\u003d5

###### PARAMETER ######
PRJ_NM\u003d${1}
ARG1\u003d${2}
ARG2\u003d${3}
#ARG3\u003d${4}
#ARG4\u003d${5}

echo \"\"
echo \"Projce      : [${PRJ_NM}] \"
if [ ${#ARG1} -gt 0 ]
then
          echo \"Parameter1 : [${#ARG1}] \"
fi
if [ ${#ARG2} -gt 0 ]
then
             echo \"Parameter2 : [${#ARG2}] \"
fi
echo \"\"

for (( i\u003d0; 1\u003c\u003d$RE_TRY_CNT; i++ ))
do
   if [ $i -ne 0 ]
   then
         echo \"[ $i ] Exec Retry...\"

   fi

    mapfile -t PRJ_PATHS \u003c \u003c(mysql -uroot -p\u0027admin!@#$\u0027 ${TS_DB} --connect_timeout\u003d5 \u003c\u003c _EOF_
SELECT
                  (CASE WHEN D.NM IS NOT NULL THEN CONCAT(\u0027/\u0027, D.NM, \u0027/\u0027, C.NM, \u0027/\u0027, B.NM)
                                  WHEN C.NM IS NOT NULL THEN CONCAT(\u0027/\u0027, C.NM, \u0027/\u0027, B.NM)
                                  WHEN B.NM IS NOT NULL THEN CONCAT(\u0027/\u0027, B.NM)
                       END)           AS PRJ_PATH
                  , IFNULL(A.PRJ_STATUS_CD,1) AS PRJ_STATUS_CD
     FROM prj           A
  INNER
     JOIN prj_grp B
         ON B.PRJ_GRP_ID \u003d A.PRJ_GRP_ID
    LEFT
    JOIN prj_grp C
        ON C.PRJ_GRP_ID \u003d B.PARENT_GRP_ID
    LEFT
    JOIN prj_grp D
        ON D.PRJ_GRP_ID \u003d C.PARENT_GRP_ID
 WHERE A.NM \u003d \u0027${PRJ_NM}\u0027
;
_EOF_
)

   RUN_RSLT\u003d$?
   if [ $RUN_RSLT -ne 0 ]
   then
      \u003e\u00262 echo \"Mysql Fail!!\"
      exit 1
   fi

   if [ ${#PRJ_PATHS[@]} -lt 2 ]
   then
      \u003e\u00262 echo \"[${PRJ_NM}] Not Exist!\"
      exit 1
   fi

   if [ ${#PRJ_PATHS[@]} -gt 2
   then
      \u003e\u00262 echo \"[${PRJ_NM}] Many Exists!\"
     exit 1
   fi

   PRJ_ROW\u003d(${PRJ_PATHS[1]})

   PRJ_STAT\u003d${PRJ_POW[1]}
   if [ $PRJ_STAT -eq 3 ]
   then
       if [ $i -eq $RE_TRY_CNT ]
       then
            \u003e\u00262 echo \"[${PRJ_NM}] Retry expired...\"
             exit 1
       fi

        echo \"[${PRJ_NM}] is Running... Retry Waiting ${RE_TRY_SEC} Seconds...\"
        sleep $RE_TRY_SEC
        continue
    fi

    echo \"\"
    echo \"[${PRJ_NM}] Status OK.\"
    echo \"\"

    PRJ_PATH\u003d${PRJ_ROW[0]}

    break
done

###### PROJECT EXEC COMMAND ######
TIME_ST\u003d${date +\"%Y-%m-%d %H:%M:%S.%N\")

echo \"\"
echo \"Start Time : $TIME_ST\"
echo \"END Time : $TIME_ED\"
echo \"Elapse Time : $(echo \"scale\u003d2;($(date +%s%N -d \"$TIME_ED\") - $(date +%s%n -d \"TIME_ST\")) / 1000000000\" | bc) Seconds\"

echo \"\"

if [ $RUN_TS -ne 0 ]
then
      \u003e\u00262 echo \"Project Fail!!\"
       exit 1
fi

echo \"Project Success.\"