from datetime import datetime, timedelta

start_time = datetime.now()  # 시작 시간 저장
# 주어진 날짜 문자열
date_string = '00000102'

# 입력된 형식의 날짜를 datetime 객체로 변환
date_object = datetime.strptime(date_string, "%Y%m%d")

one_day_minus = date_object - timedelta(days=1)

before_one_day = one_day_minus.strftime("%Y%m%d")
print(before_one_day)

print(start_time)