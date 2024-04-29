#!/bin/bash

#S3 BUCKET PATH
sap_bucket=$(grep "sapbucket" /sorc001/BATCH/COM/ENV/anenv.ini | sed 's/ = /|/g' | cut -d '|' -f2)
sap_back=$(grep "sapbackup" /sorc001/BATCH/COM/ENV/anenv.ini | sed 's/ = /|/g' | cut -d '|' -f2)
scrm_bucket=$(grep "scrmbucket" /sorc001/BATCH/COM/ENV/anenv.ini | sed 's/ = /|/g' | cut -d '|' -f2)
scrm_back=$(grep "scrmbackup" /sorc001/BATCH/COM/ENV/anenv.ini | sed 's/ = /|/g' | cut -d '|' -f2)

#SAP .CSV file backup folder mv
aws s3 mv s3://$sap_bucket/ s3://$sap_back/ --recursive --exclude="*" --include="*.CSV"

#SCRM .dat file backup folder mv
aws s3 mv s3://$scrm_bucket/ s3://$scrm_back/ --recursive --exclude="*" --include="*.dat"

# FILE DELETE PATH
delete_sap="/data001/sapdata"
delete_scrm="/data001/scrmdata"
deletegen="/sorc001/BATCH/GEN/"
deletelog="/logs001/BATCH/LOG/"

# 1WEEK AGO DELETE
find "$delete_sap" -type f -mtime +7 -daystart -exec rm {} \;
find "$delete_scrm" -type f -mtime +7 -daystart -exec rm {} \;

# 1MONTH AGO DELETE
find "$deletegen" -type f -mtime +30 -daystart -exec rm {} \;
find "$deletelog" -type f -mtime +30 -daystart -exec rm {} \;

