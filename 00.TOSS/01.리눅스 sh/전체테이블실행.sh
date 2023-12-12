#!/bin/bash

filepath=/home/tstream/project.dat
fildes=$1
date=$2
load=$3

if ! [ -e $filepath ]
   then
        echo "not exists file"
        exit 1
fi

if [ -z $fildes ]
   then
       echo "not exists columns"
       exit 1
fi

lastdt=$(date -d "${date:0:6}01 +1 month -1 day" +%Y%m%d)

grep "$fildes""=" $filepath | while read -r line
do
     newLine1=""
     newLine2=""
     newLine3=""
     newLine4=""
     newLine=""

     newLine=$(echo $line | cut -c5-17)

            if ["$load" == "I" ] || [ "$load" == "A" ]
           then
                     if [ "${newLine##*_}" != "P" ]
                    then
                           newLine1="/ADW/INI/$fildes/UID_"$newLine"_TG"
                           newLine2="/ADW/INI/$fildes/LID_"$newLine"_TG"
                       fi
             fi

            if [ "$load" == "C" ] || [ "$load" == "A" ]
           then
                  if [ "${newLine##*_}" == "P" ]
                   then
                               if [ "$newLine" == "DPVMR_AGMER_P" ] || [ "$newLine" == "DPVMR_AGMER_P" ] # 월작업
                               then
                                           if [ "$date" == "$lastdt" ]
                                          then
                                                   newLine3="/ADW/CHG/$fildes/LCM_"$newLine"_TG"
                                           fi
                                else
                                            newLine3="/ADW/CHG/$fildes/LCD_"$newLine"_TG"
                                fi
                    else
                           newLine3="/ADW/CHG/$fildes/UCD_"$newLine"_TG"
                           newLine4="/ADW/CHG/$fildes/LCD_"$newLine"_TG"
                    fi
              fi

if [ "$newLine1" != "" ]
then
tsstart 127.0.0.1 5000 $newLine1 $date
fi
if [ "$newLine2" != "" ]
then
tsstart 127.0.0.1 5000 $newLine2 $date
fi
if [ "$newLine3" != "" ]
then
tsstart 127.0.0.1 5000 $newLine3 $date
fi
if [ "$newLine4" != "" ]
then
tsstart 127.0.0.1 5000 $newLine4 $date
fi
done

echo " [ SUBJECTAREA ] : $fildes"
echo " [ DATE ] : $date"
echo " [ LOAD ] : $load"
exit 0