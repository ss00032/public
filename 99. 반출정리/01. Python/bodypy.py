
"""
(@) 공통 변수 셋팅
"""
work_schema = 'dept_dw'
work_table = 'cd_h_dong'

"""
(@) Working INSERT 쿼리 (필요한 개수만큼 변수 생성 insert_work_sql1, insert_work_sql2 ...)
"""
insert_work_sql1 = f"""
select * from dept_dw.cd_h_dong where h_dong_cd = '2917069500';
"""
##/* @@@
##insert_work_sql1 = f"""
##select * from dept_dw.cd_h_dong where h_dong_cd = '4677033000';
##"""
## @@@ */

insert_work_sql3 = f"""
select * from dept_dw.cd_h_dong where h_dong_cd = '4413132000';
"""

insert_work_sql = [insert_work_sql1, insert_work_sql2, insert_work_sql3]

"""
(@) Delete 쿼리
"""
delete_sql = f"""
select * from dept_dw.cd_h_dong where h_dong_cd = '2917069500'
"""

"""
(@) Target Insert 쿼리
"""
insert_sql = f"""
select * from dept_dw.cd_h_dong where h_dong_cd = '2917069500'
"""