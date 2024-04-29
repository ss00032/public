-- psycopg2 ��ġ���
sudo yum groupinstall "Development Tools"
sudo yum install python3-devel
sudo yum install postgresql-libs
sudo yum install postgresql-devel
sudo pip3 install psycopg2

-- Crypto ��ġ���
sudo pip3 install pycryptodome

���̽� ��� ��ġ��λ� ������ ���� ������ ��ġ ��⳴�Ŀ�� ����
�����ġ /usr/local/lib or lib64

vi /etc/bashrc -- ��ü�����
vi /home/user/.bashrc  -- user �� ����
PS1="[\u@\h \W]\\$ "
[ "$PS1" = "\\s-\\v\\\$ " ] && PS1="[\$PWD]\\$ "
���̽� �������� : python --version
���̽� ��ġ��� : which python
aws cli ��ġ��� : 
sudo apt_get update
sudo apt_get install awscli
aws configure -- AWS �ڰ�����

�� CLI�� �̿��� S3 Bucket Backup
aws s3 mv s3://dept-s3-an2-an-dwdev-sapdata/ s3://dept-s3-an2-an-dwdev-sapdata-backup/ --recursive --exclude="*" --include="*.CSV"
aws s3 mv s3://dept-s3-an2-an-dwdev-sapdata/ s3://dept-s3-an2-an-dwdev-sapdata-backup/ --recursive --exclude="*" --include="*.end"
aws s3 mv s3://dept-s3-an2-an-dwdev-scrmdwdata/ s3://dept-s3-an2-an-dwdev-scrmdwdata-backup/ --recursive --exclude="*" --include="*.dat"

�� sh ���� Secret Manager�� ��ϵ� ������ �����ϴ� ���
aws secretsmanager get-secret-value --secret-id dept-sm-an2-an-dwdev-rsc-ansla | jq -r '.SecretString | fromjson | .password'

�� Redshift �������� �� ���� �ð�
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

�� �Ʒ� ������ ���� Transaction �˻�
SELECT * FROM svv_transactions;
select a.txn_owner, a.txn_db, a.xid, a.pid, a.txn_start, a.lock_mode, a.relation as table_id,nvl(trim(c."name"),d.relname) as tablename, a.granted,b.pid as blocking_pid ,datediff(s,a.txn_start,getdate())/86400||' days '||datediff(s,a.txn_start,getdate())%86400/3600||' hrs '||datediff(s,a.txn_start,getdate())%3600/60||' mins '||datediff(s,a.txn_start,getdate())%60||' secs' as txn_duration
from svv_transactions a 
left join (select pid,relation,granted from pg_locks group by 1,2,3) b 
on a.relation=b.relation and a.granted='f' and b.granted='t' 
left join (select * from stv_tbl_perm where slice=0) c 
on a.relation=c.id 
left join pg_class d on a.relation=d.oid
where  a.relation is not null;
�� Porcess ���� (PID��)
SELECT PG_TERMINATE_BACKEND(8585);

�� nohup���� �� ����
nohup ./lid_batchrun1.sh > lid_batchrun1_20231115.log &

�� s3 ���� ��ɾ�
aws s3 ls s3://dept-s3-an2-an-dwprd-asisdw/