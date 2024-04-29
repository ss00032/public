-- psycopg2 설치방법
sudo yum groupinstall "Development Tools"
sudo yum install python3-devel
sudo yum install postgresql-libs
sudo yum install postgresql-devel
sudo pip3 install psycopg2

-- Crypto 설치방법
sudo pip3 install pycryptodome

파이썬 모듈 설치경로상 권한이 없는 문제로 설치 모듈낫파운드 에러
모듈위치 /usr/local/lib or lib64

vi /etc/bashrc -- 전체사용자
vi /home/user/.bashrc  -- user 만 적용
PS1="[\u@\h \W]\\$ "
[ "$PS1" = "\\s-\\v\\\$ " ] && PS1="[\$PWD]\\$ "
파이썬 버전정보 : python --version
파이썬 설치경로 : which python
aws cli 설치방법 : 
sudo apt_get update
sudo apt_get install awscli
aws configure -- AWS 자격증명

▶ CLI를 이용한 S3 Bucket Backup
aws s3 mv s3://dept-s3-an2-an-dwdev-sapdata/ s3://dept-s3-an2-an-dwdev-sapdata-backup/ --recursive --exclude="*" --include="*.CSV"
aws s3 mv s3://dept-s3-an2-an-dwdev-sapdata/ s3://dept-s3-an2-an-dwdev-sapdata-backup/ --recursive --exclude="*" --include="*.end"
aws s3 mv s3://dept-s3-an2-an-dwdev-scrmdwdata/ s3://dept-s3-an2-an-dwdev-scrmdwdata-backup/ --recursive --exclude="*" --include="*.dat"

▶ sh 에서 Secret Manager에 등록된 정보를 추출하는 방법
aws secretsmanager get-secret-value --secret-id dept-sm-an2-an-dwdev-rsc-ansla | jq -r '.SecretString | fromjson | .password'

▶ Redshift 수행쿼리 및 수행 시간
select
    query,
    database,
    datediff(seconds, starttime, endtime) as duration,
    querytxt,
    aborted
from stl_query
where starttime between '2023-10-17 20:30:00' and '2023-10-18 01:30:00'
    and (querytxt ilike '%insert%' or querytxt ilike '%select%' or querytxt ilike '%update%' or
        querytxt ilike '%delete%' or querytxt ilike '%copy%')
order by duration desc
;

▶ 아래 쿼리로 현재 Transaction 검색
SELECT * FROM svv_transactions;
select a.txn_owner, a.txn_db, a.xid, a.pid, a.txn_start, a.lock_mode, a.relation as table_id,nvl(trim(c."name"),d.relname) as tablename, a.granted,b.pid as blocking_pid ,datediff(s,a.txn_start,getdate())/86400||' days '||datediff(s,a.txn_start,getdate())%86400/3600||' hrs '||datediff(s,a.txn_start,getdate())%3600/60||' mins '||datediff(s,a.txn_start,getdate())%60||' secs' as txn_duration
from svv_transactions a 
left join (select pid,relation,granted from pg_locks group by 1,2,3) b 
on a.relation=b.relation and a.granted='f' and b.granted='t' 
left join (select * from stv_tbl_perm where slice=0) c 
on a.relation=c.id 
left join pg_class d on a.relation=d.oid
where  a.relation is not null;
▶ Porcess 제거 (PID값)
SELECT PG_TERMINATE_BACKEND(8585);

▶ nohup으로 쉘 실행
nohup ./lid_batchrun1.sh > lid_batchrun1_20231115.log &

▶ s3 파일 명령어
aws s3 ls s3://dept-s3-an2-an-dwprd-asisdw/