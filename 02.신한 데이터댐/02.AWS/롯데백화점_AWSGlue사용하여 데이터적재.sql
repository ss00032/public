###########################################################
# modify #
###########################################################
import sys
import datetime
import time
import zipfile
import pytz
import boto3
from boto3.session import Session
import pandas as pd
import datetime
from datetime import timedelta
from io import StringIO
import pyspark.sql.functions as f
from awsglue.transforms import *
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext, DynamicFrame
from awsglue.job import Job
from pyspark.sqlwindow import Window
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, regexp_replacd, col, lit
from pyspark.sql.functions import udf

######## 기본 셋팅 ########
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME']) ## getResolvedOptions 는 작업이 실행될 때 스크립트에 전달되는 인수에 대한 액세스를 제공하는 유틸리티 함수 인 awsglue.utils에서 제공하는 함수입니다.
sc = SparkContext() ## 스파크 컨텍스트(Spark Context)를 통해 스파크 클러스터에 접근하여 필요한 명령어를 전달하고 실행결과를 전달받게 된다
glueContext = GlueContext(sc) ## Apache Spark 플랫폼과 상호작용하기 위한 원리를 제공
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

####################### 날짜 설정 #######################
etl_dt = datetime.datetime.now(pytz.timezone('Etc/GMT-9')).strftime("%Y%m%d") ## 한국시간 설정
log_date = datetime.datetime.now(pytz.timezone('Etc/GMT-9')).strftime("%Y%m%d") ## strftime() 날짜형식 변경
# %Y : 0을 채운 10진수 표기로 4자리 년도 
# %d : 0을 채운 10진수 표기로 날짜를 표시
# %m : 0을 채운 10진수 표기로 월을 표시 => 대소문자 구별
s3client = boto3.client('s3')
con_bucket = 'ldps-tst.glue.config'
bucket = 'idps-tst.data.anl.origin'
key_path = 'meta_modify1.dat' # 파일 경로

args = getResolvedOptions(sys.argv, ['JOB_NAME'])