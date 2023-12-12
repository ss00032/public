#!/bin/bash

mapfile -t results \u003c \u003c(mysql -uroot -p\u0027admin!@#$\u0027 works50 --connect_timeout\u003d5 -N \u003c\u003c _EOF_
set names utf8
SELECT
SUBSTR(A.NM,1,3)   AS JOB_TYPE
,A.NM    AS PGM_NM
,(CASE WHEN LENGTH(A.NM) \u003d 20 THEN SUBSTR(A.NM, 5,13) ELSE NULL END)   AS TBL_NM
,(CASE WHEN D.NM IS NOT NULL THEN CONCAT(\u0027/\u0027, D.NM, \u0027/\u0027, C.NM, \u0027/\u0027, B.NM)
                 WHEN C.NM IS NOT NULL THEN CONCAT(\u0027/\u0027, C.NM, \u0027/\u0027, B.NM)
                 WHEN B.NM IS NOT NULL THEN CONCAT(\u0027/\u0027, B.NM)
       END)   AS PRJ_PATH
,U.DB_NM   AS UN_DB
,(CASE WHEN TRIM(A.DESC11) \u003d \u0027\u0027 THEN \u0027NULL\u0027 ELSE A.DESC11 END)   AS PGM_DESCS
FROM prj   A
INNER
  JOIN prj_grp   B
      ON B.PRJ_GRP_ID \u003d A.PRJ_GRP_ID
  LEFT
  JOIN prj_grp   C
     ON C.PRJ_GRP_ID \u003d B.PARENT_GRP_ID
   LEFT
  JOIN prj_grp   D
      ON D.PRJ_GRP_ID \u003d C.PARENT_GRP_ID
  LEFT
  JOIN unload2 U
      ON U.PRJ_ID \u003d A.PRJ_ID
   AND U.PRJ_VER_ID \u003d A.CUR_VER_ID
WHERE 1\u003d1
   AND (C.NM IN (\u0027CHG\u0027,\u0027INI\u0027) OR B.NM IN (\u0027COM\u0027,\u0027EAI\u0027))
 ORDER BY PRJ_PATH, PGM_NM
;
_EOF_
)

RUN_RSLT\u003d$?
if [ $RUN_RSLT -ne 0 ]
then
    \u003e\u00262 echo \"Mysql Fail!!\"
   exit 1
fi

#echo Result Cnt : ${#results[@]}

if [ ${#results[@]} -lt 1 ]
then
     \u003e\u00262 echo \"Result Not Exist!\"
    exit 1
fi

for data in \"${results[@]}\"
do
    row\u003d($data)
    JOB_TYPE\u003d${row[0]}
    PGM_NM\u003d${row[1]}
    [ ${row[2]} \u003d\u003d \"NULL\" ] \u0026\u0026 TBL_NM\u003d\"\" || TBL_NM\u003d\"${row[2]}\"
    [ ${row[3]} \u003d\u003d \"NULL\" ] \u0026\u0026 PRJ_PATH\u003d\"\" || PRJ_PATH\u003d\"${row[3]}\"
    [ ${row[4]} \u003d\u003d \"NULL\" ] \u0026\u0026 UN_DB\u003d\"\" || UN_DB\u003d\"${row[4]}\"
    [ ${row[5]} \u003d\u003d \"NULL\" ] \u0026\u0026 PGM_DESC\u003d\"\" || PGM_DESC\u003d\"${row[5]}\"

    echo \"${PGM_NM}|${JOB_TYPE}|${TBL_NM}|${PRJ_PATH}|${UN_DB}|${PGM_DESC}|\"
done

