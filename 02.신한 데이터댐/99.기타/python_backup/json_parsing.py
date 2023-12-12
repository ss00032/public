import json 

with open("json_example.json", "r", encoding="utf8") as f: 
    contents = f.read() # string 타입 
    json_data = json.loads(contents) 
    
# print(json_data) # 전체 JSON을 dict type으로 가져옴 
# print(json_data["employees"]) # Employee 정보를 조회 
# print('고객 : ',json_data["employees"][0]["firstName"]) # 첫 Employee의 이름을 출력 -> John

for employees in json_data['employees']:
    print('고객명 : ',employees)

