SELECT T10.TABLE_NAME
     , REPLACE(T10.SQL_TEXT,', ',CHR(13)||'     , ') SQL_TEXT
     , NOW()          as 최종변경일시
     , current_timestamp + '-3 DAY'          as 최종변경일시
  FROM (
        SELECT TABLE_NAME
             , 'SELECT '||STRING_AGG(COLUMN_NAME ,', ')|| CHR(13)||' FROM '||TABLE_NAME ||' T10 '||CHR(13) AS SQL_TEXT
          FROM (SELECT T10.TABLE_NAME
                     , MAX(case when UPPER(T10.DATA_TYPE) like '%CHAR%'                  then 'TRIM(T10.'||T10.COLUMN_NAME||')'
                                when UPPER(T10.DATA_TYPE) IN ('DATE','TIMESTAMP','TIME') then 'T10.'||T10.COLUMN_NAME
                                else 'NVL(T10.'||T10.COLUMN_NAME||',0)'
                           END
                          )                    as COLUMN_NAME                            
                     , MAX(CAST(T10.COL_ID AS NUMERIC)) AS COL_ORDER 
                  FROM TABLE_ETL T10
          --/*
            INNER JOIN TABLE_DEF T11
                    ON T10.TABLE_NAME = T11.TABLE_NAME
                   and T11.JIJU_YN = 'Y'
                   and T11.MKT_YN  = 'Y'
          --*/
                 GROUP BY T10.TABLE_NAME
                        , T10.COLUMN_NAME 
                 ORDER BY COL_ORDER
               ) A 
         GROUP BY TABLE_NAME
        )T10
;
CREATE TABLE public.table_cat (
   table_name varchar(30) NULL,
   column_name varchar(50) NULL,
   col_id varchar(3) NULL,
   data_type varchar(20) NULL,
   etl_except varchar(1) NULL,
   fnl_chd_dt timestamp NULL
);

CREATE TABLE public.table_etl (
   table_name varchar(30) NULL,
   column_name varchar(50) NULL,
   col_id varchar(3) NULL,
   data_type varchar(20) NULL,
   etl_except varchar(1) NULL,
   fnl_chd_dt timestamp NULL
);

CREATE TABLE public.table_def (
   table_name varchar(30) NULL,
   jiju_yn varchar(1) NULL,
   mkt_yn varchar(1) NULL
);

INSERT INTO TABLE_DEF VALUES ('채널접촉내역','N','N');
INSERT INTO TABLE_DEF VALUES ('타금융기관거래정보','N','N');
INSERT INTO TABLE_DEF VALUES ('DWM_DEPACD_RST','Y','Y');
INSERT INTO TABLE_DEF VALUES ('DWM_CUSPRDTCMMM_RST','Y','Y');
INSERT INTO TABLE_DEF VALUES ('고객기본정보','N','N');
INSERT INTO TABLE_DEF VALUES ('거래기본정보','N','N');
INSERT INTO TABLE_DEF VALUES ('DWY_YSQ_SOHOCUST','Y','Y');
INSERT INTO TABLE_DEF VALUES ('보유상품정보','N','N');
INSERT INTO TABLE_DEF VALUES ('DWS_CCO_ACCTREG','Y','Y');