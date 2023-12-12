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

######## �⺻ ���� ########
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME']) ## getResolvedOptions �� �۾��� ����� �� ��ũ��Ʈ�� ���޵Ǵ� �μ��� ���� �׼����� �����ϴ� ��ƿ��Ƽ �Լ� �� awsglue.utils���� �����ϴ� �Լ��Դϴ�.
sc = SparkContext() ## ����ũ ���ؽ�Ʈ(Spark Context)�� ���� ����ũ Ŭ�����Ϳ� �����Ͽ� �ʿ��� ��ɾ �����ϰ� �������� ���޹ް� �ȴ�
glueContext = GlueContext(sc) ## Apache Spark �÷����� ��ȣ�ۿ��ϱ� ���� ������ ����
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

####################### ��¥ ���� #######################
etl_dt = datetime.datetime.now(pytz.timezone('Etc/GMT-9')).strftime("%Y%m%d") ## �ѱ��ð� ����
log_date = datetime.datetime.now(pytz.timezone('Etc/GMT-9')).strftime("%Y%m%d") ## strftime() ��¥���� ����
# %Y : 0�� ä�� 10���� ǥ��� 4�ڸ� �⵵ 
# %d : 0�� ä�� 10���� ǥ��� ��¥�� ǥ��
# %m : 0�� ä�� 10���� ǥ��� ���� ǥ�� => ��ҹ��� ����
s3client = boto3.client('s3')
con_bucket = 'ldps-tst.glue.config'
bucket = 'idps-tst.data.anl.origin'
key_path = 'meta_modify1.dat' # ���� ���

args = getResolvedOptions(sys.argv, ['JOB_NAME'])