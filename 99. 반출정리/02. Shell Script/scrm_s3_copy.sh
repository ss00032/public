#!/bin/bash

# 변수 셋팅
s3_bucket=$(grep "scrmbucket" /sorc001/BATCH/COM/ENV/anenv.ini | sed 's/ = /|/g' | cut -d '|' -f2)
s3_key=$1 # 파일명 $1으로 변수받기
to_date=$2
file_to_check=$s3_key"_"$to_date.dat # 체크할 파일
end_to_check=$s3_key"_"$to_date.end
scrm_secret_manager=$(grep "scrm_secret_manager" /sorc001/BATCH/COM/ENV/anenv.ini | sed 's/ = /|/g' | cut -d '|' -f2)

# SFTP 정보
#sftp_server="174.100.29.97"
#sftp_user="scusran"
sftp_server=$(aws secretsmanager get-secret-value --secret-id $scrm_secret_manager | jq -r '.SecretString | fromjson | .Host')
sftp_user=$(aws secretsmanager get-secret-value --secret-id $scrm_secret_manager | jq -r '.SecretString | fromjson | .Userid')
remote_folder="/ETT_DATA/SSGAN_IF"

end_file_path="/data001/scrmdata/$end_to_check"  # end파일의 경로
dat_file_path="/data001/scrmdata/$file_to_check" # 파일의 경로
file_path="/data001/scrmdata"  # 체크할 파일의 경로
MAX_RETRIES=60
max_duration=180  # 3시간 (단위: 분)
check_interval=3  # 체크 간격 (단위: 분)
start_time=$(date +%s)  # 스크립트 시작 시간

#SFTP 명렁어 실행 및 확인
download_sftp_file() {
    sftp $sftp_user@$sftp_server <<EOF
    cd $remote_folder
    get $file_to_check /data001/scrmdata/$end_to_check
    quit
EOF
}

# 시간을 계산하고 초를 시, 분, 초 단위로 변환합니다
calculate_time() {
    local duration=$1
    local seconds=$((duration % 60))
    local minutes=$((duration / 60 % 60))
    local hours=$((duration / 3600))
    printf "%02d:%02d:%02d" $hours $minutes $seconds
}

download_start_time=$(date +%s)  # Server Download 시작 시간

#파일 체크 및 대기 루프
for ((i=1; i<=$MAX_RETRIES; i++)); do
  if download_sftp_file 2>&1 | grep -q "Fetching"; then
    # 소요 시간 계산
    download_end_time=$(date +%s)
    down_time=$((download_end_time - download_start_time))

    # 소요 시간을 출력 형식으로 변환하여 표시
    down_format_time=$(calculate_time $down_time)
    echo "======================================"
    echo "파일 다운로드 완료"
    echo "다운로드 소요 시간: $down_format_time"
    echo "======================================"
    break
  else
    echo "파일이 존재하지 않습니다... $i/$MAX_RETRIES 시도..."
    sleep $((check_interval * 60))  # 일정 시간 대기
  fi
done

if [ $i -gt $MAX_RETRIES ]; then
  echo "파일이 존재하지 않거나 다운로드 실패"
  exit 1
fi

upload_start_time=$(date +%s)  # S3 UPLOAD 시작 시간

while true; do
    current_time=$(date +%s)
    elapsed_time=$((current_time - start_time))

    rm -f $dat_file_path
    #파일 갯수 체크
    filecnt=$(find /data001/scrmdata/$s3_key"_"$to_date* -type f | wc -l)

    # 파일 체크
    if [ -f "$end_file_path" ]; then
        if [ $filecnt -eq 1 ]; then
            echo "======================================"
            echo "파일이 1건 존재합니다."
            iconv -f euc-kr -t UTF-8 $end_file_path -o $dat_file_path
            rm -f $end_file_path
            aws s3 cp $file_path/$file_to_check s3://$s3_bucket/$file_to_check
	    rm -f $dat_file_path
        else
            echo "파일을 확인하세요."
            exit 1
        fi
            # 소요 시간 계산
            upload_end_time=$(date +%s)
            upload_time=$((upload_end_time - upload_start_time))

            # 소요 시간을 출력 형식으로 변환하여 표시
            up_time=$(calculate_time $upload_time)
            echo "업로드 소요 시간: $up_time"
	    echo "======================================"
            exit 0
    else
        echo "파일이 존재하지 않습니다."
    fi

    # 시간 체크
    if [ "$elapsed_time" -ge $((max_duration * 60)) ]; then
        echo "오류: 3시간이 지났습니다."
        exit 1
    fi

    sleep $((check_interval * 60))  # 일정 시간 대기
done

