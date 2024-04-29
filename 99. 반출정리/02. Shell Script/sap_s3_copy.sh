#!/bin/bash

# 변수 셋팅
s3_bucket=$(grep "sapbucket" /sorc001/BATCH/COM/ENV/anenv.ini | sed 's/ = /|/g' | cut -d '|' -f2)
s3_key=$1 # 파일명 $1으로 변수받기
to_date=$2
end_file=$s3_key"_"$to_date.end

end_file_path="/data001/sapdata/$end_file"  # end 파일의 경로
file_path="/data001/sapdata"  # 체크할 파일의 경로
max_duration=180  # 3시간 (단위: 분)
check_interval=5  # 체크 간격 (단위: 분)
start_time=$(date +%s)  # 스크립트 시작 시간

while true; do
    current_time=$(date +%s)
    elapsed_time=$((current_time - start_time))

    #파일 갯수 체크
    filecnt=$(find /data001/sapdata/$s3_key"_"$to_date* -type f | wc -l)
    
    # end 파일 체크 X
    if [ $s3_key == 'ZACS2020' ]; then
	aws s3 cp $file_path/ s3://$s3_bucket/ --recursive --exclude "*" --include "$s3_key"_"$to_date*" --exclude "*.end"
	exit 0
    # end 파일 체크
    elif [ -f "$end_file_path" ]; then
        # 파일 체크
        if [ $filecnt -eq 2 ] && [ $s3_key != 'ZPCT0002' ]; then
            echo "파일이 1건 존재합니다."
            aws s3 cp $file_path/$s3_key"_"$to_date.CSV s3://$s3_bucket/$s3_key"_"$to_date.CSV
        elif [ $filecnt -gt 2 ]; then
            csvfilecnt=$(expr $filecnt - 1)
            echo "파일이 $csvfilecnt건 존재합니다."
            for (( counter = 1; counter < $filecnt; counter++)); do
                #cnt=$(printf "%02d\n" $counter) #01 02...
                            cnt=$(printf "%d\n" $counter) #1 2...
                aws s3 cp $file_path/$s3_key"_"$to_date"_"$cnt.CSV s3://$s3_bucket/$s3_key"_"$to_date"_"$cnt.CSV
            done
        elif [ $s3_key == 'ZPCT0002' ]; then
            for (( counter = 1; counter < 6; counter++)); do
                            cnt=$(printf "%d\n" $counter) #1 2...
                aws s3 cp $file_path/$s3_key"_"$to_date"_"$cnt.CSV s3://$s3_bucket/$s3_key"_"$to_date"_"$cnt.CSV
            done
        else
            echo "파일을 확인하세요."
            exit 1
        fi
            exit 0
    else
        echo "END 파일이 존재하지 않습니다."
    fi

    # 시간 체크
    if [ "$elapsed_time" -ge $((max_duration * 60)) ]; then
        echo "오류: 3시간이 지났습니다."
        exit 1
    fi

    sleep $((check_interval * 60))  # 일정 시간 대기
done
