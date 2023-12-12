#!/bin/bash

filepath\u003d/home/tstream/project.dat
fildes\u003d$1
date\u003d$2
load\u003d$3

if ! [ -e $filepath ]
   then
        echo \"not exists file\"
        exit 1
fi

if [ -z $fildes ]
   then
       echo \"not exists columns\"
       exit 1
fi

lastdt\u003d$(date -d \"${date:0:6}01 +1 month -1 day\" +%Y%m%d)

grep \"$fildes\"\"\u003d\" $filepath | while read -r line
do
     newLine1\u003d\"\"
     newLine\u003d\"\"

     newLine\u003d$(echo $line | cut -c5-17)

                  if [ \"${newLine##*_}\" \u003d\u003d \"P\" ]
                   then
                               if [ \"$newLine\" \u003d\u003d \"DPVMR_AGMER_P\" ] || [ \"$newLine\" \u003d\u003d \"DPVMR_AGMER_P\" ] # 월작업
                               then
                                           if [ \"$date\" \u003d\u003d \"$lastdt\" ]
                                          then
                                                   newLine1\u003d\"/ADW/CHG/$fildes/LCM_\"$newLine\"_TG\"
                                           fi
                                else
                                            newLine1\u003d\"/ADW/CHG/$fildes/LCD_\"$newLine\"_TG\"
                                fi
                    fi

if [ \"$newLine1\" !\u003d \"\" ]
then
tsstart 127.0.0.1 5000 $newLine1 $date
fi

done

echo \" [ SUBJECTAREA ] : $fildes\"
echo \" [ DATE ] : $date\"
echo \" [ LOAD ] : $load\"
exit 0