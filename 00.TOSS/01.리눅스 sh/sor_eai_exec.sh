#!/bin/bash

echo \"\"
echo \" \u003c\u003c SOR EAI Interface Batch Execute! \u003e\u003e \"
echo \"\"

printUsage () {
   SH\u003d$(basename $0)
   if [[ \"$1\" ]]; then
      echo \"$1\"
      echo \"\"
   fi
   echo \"Usage : $SH [EAI INTERFACE ID] [JOB DATE] \"
   echo \"   EX : $SH SORFF00001SOR 20201231 \"
   exit 1
}

if [ $# -ne 2 ]
then
   printUsage \"Error : Invalid Arguments!\"
fi

IF_ID\u003d$1
JOB_DATE\u003d$2

if [ ${#IF_ID} -ne 13 ]
then
   printUsage \"Error : Interface ID Argument Invalid! [${IF_ID}]\"
fi

if [ ${#JOB_DATE} -ne 8 ]
then
   printUsage \"Error : Job Date Argument Invalid! [${JOB_DATE}]\"
fi

if [ \"${JOB_DATE}\" !\u003d \"${date +%Y%m%d -d ${JOB_DATE} 2\u003e\u00261)\" ]
then
   printUsage \"Error : Job Date Argument Invalid Date! [${JOB_DATE}]\"
fi

EAI_URL\u003d\"http://eai-bat.tossbank.bz:10041/BATWeb/EaiBatCall?eaibatMsg\u003d\"
EAI_DIR_SND\u003d\"/data01/tstream/etl/eai/snd\"

FILE_NAME\u003d${IF_ID}_${JOB_DATE}.dat
FILE_PATH\u003d${EAI_DIR_SND}/${FILE_NAME}

echo \"EAI Send File \u003d [${FILE_PATH}] \"

if [ ! -f ${FILE_PATH} ]
then
   echo \"File Not Exists!\"
   echo \"EAI Batch Fail!\"
   exit 1
fi

#if [ ! -s ${FILE_PATH} ]
#then
#   echo \"File Size is zero!\"
#   echo \"EAI Batch Fail!\"
#  exit 1
#fi

FILE_CNT\u003d`wc -l \u003c ${FILE_PATH}`

echo \"File Rows\u003d[${FILE_CNT}]\"

#if [ ${FILE_CNT} -eq 0 ]
#then
#   echo \"File is Empty! \"
#   echo \"EAI Batch Fail!\"
#   exit 1
#fi

IF_ID_S19\u003d$(printf \"%-19s\" ${IF_ID})
BASE_DT_S8\u003d$2
FILE_CTNT_N10\u003d$(printf \"%0*d\" 10 ${FILE_CNT})
STA_S1\u003d0
ETC_REF_CTNT_S100\u003d$(printf \"%-100s\" ${FILE_NAME})

EAI_MSG\u003d\"${IF_ID_S19}${BASEDT}${FILE_CTNT_N10}${STA_S1}${ETC_REF_CTNT_S100}@@\"

echo \"\"
echo \"IF_ID_S19 \u003d [${IF_ID_S19}]\"
echo \"BASE_DT \u003d [${BASEDT}]\"
echo \"FILE_CTNT_N10 \u003d [${FILE_CTNT_N10}]\"
echo \"STA_S1 \u003d [${STA_S1}]\"
echo \"ETC_REF_CTNT_S100 \u003d [${ETC_REF_CTNT_S100}]\"
echo \"EAI_MSG \u003d [${EAI_MSG}]\"
echo \"\"

EAI_RESP\u003d$(cur1 -sG ${EAI_URL}${EAI_MSG// /%20})
CURL_RESULT\u003d$?

echo \"EAI Response : [${CURL_RESULT}][${EQI_RESP}]\"

if [ $CURL_RESULT -ne 0 ] || [[ \"$EAI_RESP\" !\u003d \"$IF_ID\"*\"@@\" ]]
then
   echo \"EAI Batch Error Response!\"
   echo \"Eai Batch Fail!\"
   exit 1
fi


STAT_RSP\u003d${EAI_RESP:37:1}

echo \"EAI Result Stat : $STAT_RSP\"

if [ \"$STAT_RSP\" !\u003d \"1\" ]
then
   ERR_MSG\u003d${EAI_RESP:40}
   echo \"EAI Error Message : [${ERR_MSG%%   *@@}]\"
   echo \"EAI Batch Fail!\"
   exit 1
fi

EAI_MSG_ID\u003d${EAI_RESP:41:36}

echo \"EAI Message ID : [$EAI_MSG_ID]\"
echo \"EAI Batch Success!\"

exit 0