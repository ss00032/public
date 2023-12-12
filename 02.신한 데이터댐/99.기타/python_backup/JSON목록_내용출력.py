#open(
#  file,            # 필수 매개변수, 파일의 경로
#  mode = 'r',      # 선택적 매개변수, 'rt' (읽기용, text mode)가 기본값
#  buffering = -1,  # 선택적 매개변수, 버퍼링 정책 (0 : binary mode 시 버퍼링 미수행, 
                   # 1: 텍스트 모드 시 개행문자(\n)을 만날 때까지 버퍼링)
#  encoding = None, # 선택적 매개변수, 문자 인코딩 방식 (예: 'utf-8', 'cp949', 'latin' 등)
#  errors = None,   # 선택적 매개변수, 텍스트 모드 시 에러 처리 (예: 'ignore' 에러 무시)
#  newline = None,  # 선택적 매개변수, 줄바꿈 처리 (None, '\n', '\r', '\r\n')
#  closefd = True,  # 선택적 매개변수, False 입력 시 파일 닫더라도 파일 기술자 계속 열어둠
#  opener = None    # 선택적 매개변수, 파일을 여는 함수를 직저 구현 시 사용
#)

import glob

list = 0
path_dir = 'C:\신한 데이터댐\pythonProject\새 폴더\*'

file_list = glob.glob(path_dir)
for i in file_list:
    MyFile = open(file_list[list], 'r')
    MyString = MyFile.read()
    print(file_list[list],MyString, sep='\n\n')
    list += 1


