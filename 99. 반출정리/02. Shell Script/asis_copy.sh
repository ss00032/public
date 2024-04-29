#!/bin/bash

# 변수 설정
s3_bucket=$(grep "asisdwbucket" /sorc001/BATCH/COM/ENV/anenv.ini | sed 's/ = /|/g' | cut -d '|' -f2)
s3_key=$1/$2.csv
iam_role=$(grep "arn_s3" /sorc001/BATCH/COM/ENV/anenv.ini | sed 's/ = /|/g' | cut -d '|' -f2)
redshift_secret_manager=$(grep "redshift_secret_manager" /sorc001/BATCH/COM/ENV/anenv.ini | sed 's/ = /|/g' | cut -d '|' -f2)

# Redshift 접속정보
redshift_host=$(aws secretsmanager get-secret-value --secret-id $redshift_secret_manager | jq -r '.SecretString | fromjson | .host')
redshift_port=$(aws secretsmanager get-secret-value --secret-id $redshift_secret_manager | jq -r '.SecretString | fromjson | .port')
redshift_user=$(aws secretsmanager get-secret-value --secret-id $redshift_secret_manager | jq -r '.SecretString | fromjson | .username')
encrypwd=$(aws secretsmanager get-secret-value --secret-id $redshift_secret_manager | jq -r '.SecretString | fromjson | .password')
redshift_password=$(python3 /sorc001/BATCH/COM/PYTHON/decrypass.py $encrypwd)
redshift_database="bludbev"
redshift_table="ANSTG.$1"

start_time=$(date +%s)
#DELETE FROM $redshift_table;
# S3에서 파일을 복사하여 Redshift에 삽입
psql "host=$redshift_host port=$redshift_port dbname=$redshift_database user=$redshift_user password=$redshift_password" << EOF
COPY $redshift_table
FROM 's3://$s3_bucket/$s3_key'
IAM_ROLE '$iam_role'
FORMAT AS CSV DELIMITER ';'
IGNOREHEADER 1 ENCODING UTF8
REGION AS 'ap-northeast-2'
EOF
# QUOTE '"'
#BLANKSASNULL EMPTYASNULL
# 시간을 계산하고 초를 시, 분, 초 단위로 변환합니다
calculate_time() {
    local duration=$1
    local seconds=$((duration % 60))
    local minutes=$((duration / 60 % 60))
    local hours=$((duration / 3600))
    printf "%02d:%02d:%02d" $hours $minutes $seconds
}

# 소요 시간 계산
current_time=$(date +%s)
elapsed_time=$((current_time - start_time))

# 소요 시간을 출력 형식으로 변환하여 표시
formatted_time=$(calculate_time $elapsed_time)
echo "소요 시간: $formatted_time"

