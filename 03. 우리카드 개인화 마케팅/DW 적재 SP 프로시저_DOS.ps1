Add-Type -AssemblyName System.Data.OracleClient

$PSDefaultParameterValues['*:Encoding'] = 'Default'

# 개인화마케팅 DB 개발
$username = ""
$password = ""
$data_source = ""
$connection_string = "User Id=$username;Password=$password;Data Source=$data_source"

# 개인화마케팅 DB 개발_TEMP
$tmp_username = ""
$tmp_password = ""
$tmp_data_source = ""
$tmp_connection_string = "User Id=$tmp_username;Password=$tmp_password;Data Source=$tmp_data_source"

# 카드계정계 DB 개발
$tsdb_username = ""
$tsdb_password = ""
$tsdb_data_source = ""
$tsdb_connection_string = "User Id=$tsdb_username;Password=$tsdb_password;Data Source=$tsdb_data_source"

# 카드 DW DB 개발 (운영용)
$dw_username = ""
$dw_password = ""
$dw_data_source = ""
$dw_connection_string = "User Id=$dw_username;Password=$dw_password;Data Source=$dw_data_source"


try {
     $ExcelObj = new-Object -Comobject Excel.Application
     $ExcelObj.visible=$false

     $ExcelWorkBook = $ExcelObj.Workbooks.Open("D:\00.우리카드 개인화마케팅 통합플랫폼 구축\00. 업무\999. ETL영역 관련\ETL영역_관련문서.xlsx")
     $ExcelWorkSheet = $ExcelWorkBook.Sheets.Item("파워쉘작업_마케팅추출") # DW적재 프로그램
     $rowcount=$ExcelWorkSheet.UsedRange.Rows.Count

     $selfile=@()
     $crefile=@()
     $loadfile=@()

     for ($i=2; $i -le $rowcount; $i++) {
         $ExcelOwner = $ExcelWorkSheet.cells.Item($i, 5).value2
         $ExcelTable = $ExcelWorkSheet.cells.Item($i, 6).value2
         $ExcelTarOwner = $ExcelWorkSheet.cells.Item($i, 7).value2
         $ExcelTarTable = $ExcelWorkSheet.cells.Item($i, 8).value2
         $ExcelTableTemp = $ExcelTarTable
         $ExcelChgTable = $ExcelWorkSheet.cells.Item($i, 9).value2
         #if ($ExcelTarTable -eq 'WCDIFS52TH') {
         #   $ExcelTable = $ExcelTable + '_1001'
         #} else
         
         if ($ExcelTarTable -eq 'WCDIFS17TH' -or $ExcelTarTable -eq 'WCDIFS18TH') {
            $ExcelTable = $ExcelTable + '_1'
         }
         Write-Host "--------------------" $ExcelTarOwner"."$ExcelTarTable
         #$statement = "select distinct owner, table_name, column_name, column_id from all_tab_columns where table_name = '" + $ExcelData + "' order by column_id"
         $statementpmp = "SELECT INDEX_OWNER
                         `     , INDEX_NAME
                         `     , TABLE_OWNER
                         `     , TABLE_NAME
                         `     , COLUMN_NAME
                         `     , COLUMN_POSITION
                         `  FROM ALL_IND_COLUMNS
                         ` WHERE INDEX_OWNER = '" + $ExcelTarOwner + "'
                         `   AND INDEX_NAME = 'UX_" + $ExcelTarTable + "_01'
                         ` ORDER BY COLUMN_POSITION ASC"
         $statement = "SELECT
                      `       A.OWNER
                      `     , A.TABLE_NAME
                      `     , B.COMMENTS       AS TABLE_COMMENTS
                      `     , A.COLUMN_NAME
                      `     , nvl(C.COMMENTS,'없음')       AS COLUMN_COMMENTS
                      `     , A.DATA_TYPE
                      `     , A.DATA_LENGTH
                      `     , A.DATA_PRECISION
                      `     , nvl(A.DATA_SCALE,'-999')
                      `     , A.NULLABLE
                      `     , A.COLUMN_ID
                      `     , A.DEFAULT_LENGTH
                      `     , TRIM(REPLACE(DBMS_XMLGEN.GETXMLTYPE('SELECT DATA_DEFAULT
                      `                                              FROM ALL_TAB_COLS
                      `                                             WHERE OWNER = '''||A.OWNER||'''
                      `                                               AND TABLE_NAME = '''||A.TABLE_NAME||'''
                      `                                               AND COLUMN_NAME = '''||A.COLUMN_NAME||''' ').EXTRACT('//text()'),'&apos,','''')) AS DATA_DEFAULT
                      `     , (CASE WHEN D.COLUMN_NAME IS NULL THEN 'N' ELSE 'Y' END) AS PK_YN
                      `  FROM ALL_TAB_COLS A
                      `  LEFT
                      `  JOIN ALL_TAB_COMMENTS B
                      `    ON B.OWNER = A.OWNER
                      `   AND B.TABLE_NAME = A.TABLE_NAME
                      `  LEFT
                      `  JOIN ALL_COL_COMMENTS C
                      `    ON C.OWNER = A.OWNER
                      `   AND C.TABLE_NAME = A.TABLE_NAME
                      `   AND C.COLUMN_NAME = A.COLUMN_NAME
                      `  LEFT
                      `  JOIN ALL_IND_COLUMNS D
                      `    ON D.TABLE_OWNER = A.OWNER
                      `   AND D.TABLE_NAME = A.TABLE_NAME
                      `   AND D.COLUMN_NAME = A.COLUMN_NAME
                      `   AND (D.INDEX_NAME LIKE 'PK%' OR D.INDEX_NAME LIKE '%PK')
                      ` WHERE A.OWNER = '" + $ExcelTarOwner + "'
                      `   AND A.TABLE_NAME = '" + $ExcelTarTable + "'
                      `   AND A.COLUMN_NAME NOT LIKE '%PSWD'
                      `   AND A.HIDDEN_COLUMN = 'NO'
                      ` ORDER BY A.OWNER
                      `        , A.TABLE_NAME
                      `        , A.COLUMN_ID"

         $statement_temp = "SELECT
                      `       A.OWNER
                      `     , A.TABLE_NAME
                      `     , B.COMMENTS       AS TABLE_COMMENTS
                      `     , A.COLUMN_NAME
                      `     , nvl(C.COMMENTS,'없음')       AS COLUMN_COMMENTS
                      `     , A.DATA_TYPE
                      `     , A.DATA_LENGTH
                      `     , A.DATA_PRECISION
                      `     , nvl(A.DATA_SCALE,'-999')
                      `     , A.NULLABLE
                      `     , A.COLUMN_ID
                      `     , A.DEFAULT_LENGTH
                      `     , TRIM(REPLACE(DBMS_XMLGEN.GETXMLTYPE('SELECT DATA_DEFAULT
                      `                                              FROM ALL_TAB_COLS
                      `                                             WHERE OWNER = '''||A.OWNER||'''
                      `                                               AND TABLE_NAME = '''||A.TABLE_NAME||'''
                      `                                               AND COLUMN_NAME = '''||A.COLUMN_NAME||''' ').EXTRACT('//text()'),'&apos,','''')) AS DATA_DEFAULT
                      `     , (CASE WHEN D.COLUMN_NAME IS NULL THEN 'N' ELSE 'Y' END) AS PK_YN
                      `  FROM ALL_TAB_COLS A
                      `  LEFT
                      `  JOIN ALL_TAB_COMMENTS B
                      `    ON B.OWNER = A.OWNER
                      `   AND B.TABLE_NAME = A.TABLE_NAME
                      `  LEFT
                      `  JOIN ALL_COL_COMMENTS C
                      `    ON C.OWNER = A.OWNER
                      `   AND C.TABLE_NAME = A.TABLE_NAME
                      `   AND C.COLUMN_NAME = A.COLUMN_NAME
                      `  LEFT
                      `  JOIN ALL_IND_COLUMNS D
                      `    ON D.TABLE_OWNER = A.OWNER
                      `   AND D.TABLE_NAME = A.TABLE_NAME
                      `   AND D.COLUMN_NAME = A.COLUMN_NAME
                      `   AND (D.INDEX_NAME LIKE 'PK%' OR D.INDEX_NAME LIKE '%PK')
                      ` WHERE A.OWNER = '" + $ExcelTarOwner + "'
                      `   AND A.TABLE_NAME = '" + $ExcelTableTemp + "'
                      `   AND A.COLUMN_NAME NOT LIKE '%PSWD'
                      `   AND A.HIDDEN_COLUMN = 'NO'
                      ` ORDER BY A.OWNER
                      `        , A.TABLE_NAME
                      `        , A.COLUMN_ID"

         $pmpTable = New-Object System.Data.OracleClient.OracleConnection($dw_connection_string)
         $pmpTable.Open()

         $cmd = $pmpTable.CreateCommand()
         $cmd.CommandText = $statementpmp

         $result = $cmd.ExecuteReader()
         $colArr=@()
         $colPkArr=@()
         $colArrTemp=@()
         $colloadArr=@()
         $program=@()

         while ($result.Read())
         {
          $indOwner = $result.GetString(0)
          $indNm = $result.GetString(1)
          $tblOnwer = $result.GetString(2)
          $tblNm = $result.GetString(3)
          $columnnm = $result.GetString(4)
          $col_id = $result.GetValue(5)

          #DELETE를 위한 PK 조인
          if($col_id -eq 1) {
             $colPkArr += "            WHERE B." + $columnnm.PadRight(16) + " = A." + $columnnm + "`n"
          } else {
             $colPkArr += "             AND B." + $columnnm.PadRight(16) + " = A." + $columnnm + "`n"
          }
         }

         $conTable = New-Object System.Data.OracleClient.OracleConnection($dw_connection_string)
         $conTable.Open()

         $cmd = $conTable.CreateCommand()
         $cmd.CommandText = $statement

         $result = $cmd.ExecuteReader()

         while ($result.Read())
         {
          $Owner = $result.GetString(0)
          $table = $result.GetString(1)
          #$tablenm = $result.GetString(2)
          $column = $result.GetString(3)
          $columnnm = $result.GetString(4)
          $datatype = $result.GetString(5)
          $datalength = $result.GetValue(6)
          $dataprecision = $result.GetValue(7)
          $datascale = $result.GetValue(8)
          #$nullable = $result.GetString(9)
          $col_id = $result.GetValue(10)
          #$deflength = $result.GetValue(11)
          $datadef = $result.GetString(12)
          $pkyn = $result.GetString(13)

          #INSERT 레이아웃 설정
          if($col_id -eq 1) {
            $colArr += "            " + $column.PadRight(88) + "/* " + $columnnm + " */`n"
          }else {
            $colArr += "         , " + $column.PadRight(88) + "/* " + $columnnm + " */`n"
          }
         }

         $conTemp = New-Object System.Data.OracleClient.OracleConnection($dw_connection_string)
         $conTemp.Open()

         $cmd = $conTemp.CreateCommand()
         $cmd.CommandText = $statement_temp

         $result = $cmd.ExecuteReader()

         while ($result.Read())
         {
          $Owner = $result.GetString(0)
          $table = $result.GetString(1)
          #$tablenm = $result.GetString(2)
          $column = $result.GetString(3)
          $columnnm = $result.GetString(4)
          $datatype = $result.GetString(5)
          $datalength = $result.GetValue(6)
          $dataprecision = $result.GetValue(7)
          $datascale = $result.GetValue(8)
          #$nullable = $result.GetString(9)
          $col_id = $result.GetValue(10)
          #$deflength = $result.GetValue(11)
          $datadef = $result.GetString(12)
          #$pkyn = $result.GetString(13)

          #INSERT 레이아웃 설정
          if($col_id -eq 1) {
            $colArrTemp += "            " + $column.PadRight(88) + "/* " + $columnnm + " */`n"
          }else {
            $colArrTemp += "         , " + $column.PadRight(88) + "/* " + $columnnm + " */`n"
          }
         }

         #변경적재가 아닌 테이블은 생성하지 않는다.
         if ($ExcelChgTable -eq 'X') {
         
         } else {
           #요일별로 다른 테이블 조회하는 테이블 방어로직 (현재 사용안함?)
           if ($ExcelTarTable -eq 'WCDIFS17TH' -or $ExcelTarTable -eq 'WCDIFS18TH' -or $ExcelTarTable -eq 'WCDIFS52TH') {

$program  = "CREATE OR REPLACE PROCEDURE CDAPP.SP_" + $ExcelTarTable + "
`  (
`    P_STD_DT      IN VARCHAR2                                                                       /* 기준일(일 작업인 경우 필수 항목) */
`  , P_HIRK_SEQ_INF IN VARCHAR2    DEFAULT NULL                                                      /* 최상위 시퀀스정보 */
`  , P_POSI_SEQ_INF IN VARCHAR2    DEFAULT NULL                                                      /* 바로위 시퀀스정보 */
`  )
`IS
`
`  /*----------------------------------------------------------------------------------------------*/
`  /*      전체 공통 변수 선언                                                                     */
`  /*----------------------------------------------------------------------------------------------*/
`  L_STA_DH             VARCHAR2(14);                                                                /* 시작일시 */
`  L_PGM_ID             VARCHAR2(50);                                                                /* 프로그램ID */
`  L_XTRC_CN            NUMBER(18);                                                                  /* 추출건수 */
`  L_LOAD_CN            NUMBER(18);                                                                  /* 적재건수 */
`  L_DEL_CN             NUMBER(18);                                                                  /* 삭제건수 */
`  L_PARAM_INF          VARCHAR2(50);                                                                /* 파라미터 */
`  L_PRCD_STP_INF       VARCHAR2(50);                                                                /* 작업단계 */
`  L_COMRET             NUMBER(1);                                                                   /* SF_RSDWCOMN 의 RETURN 값 */
`  L_RETURN             NUMBER(1);                                                                   /* SF_WCDCM001TH 의 RETURN 값 */
`  L_SQLERRM            VARCHAR2(500);                                                               /*  오라클 에러메세지 */
`  L_TRC_PARTITION      VARCHAR2(200);                                                               /* PARTITION TRUNCATE OUTPUT PARAMETER */
`                                                                                                 
`  L_LBLA               VARCHAR2(2);                                                                 /* LB/LA */
`  L_STD_YM             VARCHAR2(6);                                                                 /* LB/LA */
`
`
`BEGIN
`
`  /*----------------------------------------------------------------------------------------------*/
`  /*     전체 공통 변수 초기 SETTING                                                              */
`  /*----------------------------------------------------------------------------------------------*/
`  SELECT TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') INTO L_STA_DH FROM DUAL;                               /* 시작일시 */
`  L_PGM_ID        := 'SP_" + ($ExcelTarTable + "';").PadRight(75) + "/* 프로그램ID */
`  L_XTRC_CN       := 0;                                                                             /* 추출건수 */
`  L_LOAD_CN       := 0;                                                                             /* 적재건수 */
`  L_DEL_CN        := 0;                                                                             /* 삭제건수 */
`  L_PARAM_INF     := P_STD_DT;                                                                      /* 파라미터 */
`  L_PRCD_STP_INF := '*';                                                                            /* 작업단계 */
`  L_LBLA         := 'LA';
`  L_STD_YM       := SUBSTR(P_STD_DT,1,6);
`
`  /*----------------------------------------------------------------------------------------------*/
`  /*       등록되지 않은 PROGRAM 인 경우 EXCEPTION 처리 및 종료                                   */
`  /*----------------------------------------------------------------------------------------------*/
`  L_COMRET := SF_RSDWCOMN(L_PGM_ID, P_STD_DT);
`
`  IF L_COMRET <> 0 THEN
`    L_LBLA := 'LB';
`  END IF;
`
`  CASE WHEN L_COMRET = 0
`       THEN NULL;
`       WHEN L_COMRET = 1 THEN
`          RAISE_APPLICATION_ERROR(-20001,'미등록 PGM_ID입니다');
`       WHEN L_COMRET = 2 THEN
`          RAISE_APPLICATION_ERROR(-20002,'선행프로그램이 미실행입니다');
`       WHEN L_COMRET = 3 THEN
`          RAISE_APPLICATION_ERROR(-20003,'동일기간 중복 실행입니다');
`  END CASE;
`
`  /*----------------------------------------------------------------------------------------------*/
`  /*       시작 LOG SETTING/시작시간 확인                                                         */
`  /*----------------------------------------------------------------------------------------------*/
`  IF SF_WCDCM901TH ('LB' ,NULL  ,L_PGM_ID ,L_PARAM_INF,L_STA_DH ,L_PRCD_STP_INF,'RUNNING', L_XTRC_CN, L_LOAD_CN ,L_DEL_CN ,L_PARAM_INF ,P_HIRK_SEQ_INF ,P_POSI_SEQ_INF ) != 0 THEN
`     NULL;
`  END IF;
`
`    DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 작업시작 = [' || L_STA_DH || ']');
`    DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 기준년월 = [' || P_STD_DT || ']');
`    DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 변환구분 = [' || P_HIRK_SEQ_INF || ']');
`
`  /*----------------------------------------------------------------------------------------------*/
`  /*             BIZ LOGIC 처리 시작                                                              */
`  /*----------------------------------------------------------------------------------------------*/
`
`  /*----------------------------------------------------------------------------------------------*/
`  /* STEP01 : TEMP TABLE -> TARTGET TABLE JOIN DELETE                                             */
`  /*----------------------------------------------------------------------------------------------*/
`  L_PRCD_STP_INF := '01';
`  L_RETURN := SF_WCDCM901TH ('LU' ,NULL  ,L_PGM_ID ,L_PARAM_INF,L_STA_DH ,L_PRCD_STP_INF);
`  DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 작업 STEP = [' || L_PRCD_STP_INF || ']['|| TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS')||']');
`
`    DELETE /*+ LEADING(B A) USE_HASH(B A) PARALLEL(A 32) PARALLEL(B 32) */
`      FROM " + $ExcelTarTable + " A
`     WHERE EXISTS (
`           SELECT /*+ PARALLEL(B 4) FULL(B) */ 1
`             FROM CDTMP." + $ExcelTarTable + "_T01 B
$colPkArr        )
`   ;
`
`	 L_DEL_CN := SQL%ROWCOUNT;
`	 DBMS_OUTPUT.PUT_LINE('삭제건수 =[' || L_DEL_CN || ']');
`
`COMMIT;
`
`  /*----------------------------------------------------------------------------------------------*/
`  /* STEP02 : TEMP TABLE -> TARTGET TABLE INSERT                                                  */
`  /*----------------------------------------------------------------------------------------------*/
`  L_PRCD_STP_INF := '02';
`  L_RETURN := SF_WCDCM901TH ('LU' ,NULL  ,L_PGM_ID ,L_PARAM_INF,L_STA_DH ,L_PRCD_STP_INF);
`  DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 작업 STEP = [' || L_PRCD_STP_INF || ']['|| TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS')||']');
`
`
`  	 INSERT /*+ APPEND PARALLEL(Z 8) */ INTO CDOWN." + $ExcelTarTable + "
`          (
$colArr          )
`  	 SELECT /*+ LEADING(A) PARALLEL(A 8) FULL(A) USE_HASH(A) */
$colArrTemp  	   FROM CDTMP." + $ExcelTarTable + "_T01
`             ;
`
`	L_LOAD_CN := SQL%ROWCOUNT;
`	DBMS_OUTPUT.PUT_LINE('적재건수 =[' || L_LOAD_CN || ']');
`
`  COMMIT;
`
`  /*----------------------------------------------------------------------------------------------*/
`  /*       종료 LOG SETTING/종료시간 확인                                                         */
`  /*----------------------------------------------------------------------------------------------*/
`  L_RETURN := SF_WCDCM901TH ('LA' ,NULL ,L_PGM_ID ,L_PARAM_INF,L_STA_DH ,L_PRCD_STP_INF,'Finished', L_XTRC_CN, L_LOAD_CN ,L_DEL_CN ,L_PARAM_INF ,P_HIRK_SEQ_INF ,P_POSI_SEQ_INF ) ;
`  DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 작업종료 = [' || TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') || ']');
`  DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 기준년월 = [' || P_STD_DT || ']');
`
`EXCEPTION
`  /* 예외발생 :  ABORTED로 종료 */
`  WHEN OTHERS THEN
`       L_SQLERRM := SQLERRM ;
`       DBMS_OUTPUT.PUT_LINE('L_SQLERRM='||L_SQLERRM);
`       L_RETURN := SF_WCDCM901TH (L_LBLA, NULL ,L_PGM_ID ,P_STD_DT,L_STA_DH ,L_PRCD_STP_INF,'Aborted', L_LOAD_CN, L_LOAD_CN ,L_DEL_CN ,'P_STD_DT='||P_STD_DT ,P_HIRK_SEQ_INF ,P_POSI_SEQ_INF ,L_SQLERRM) ;
`       DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : L_PRCD_STP_INF = [' || L_PRCD_STP_INF || ']');
`       DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : EXCEPTION = [기타], ERR_CODE=[' || TO_CHAR(SQLCODE) || ']');
`       DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : EXCEPTION = [기타], ERR_MSG= [' || L_SQLERRM        || ']');
`       ROLLBACK;
`       --RAISE_APPLICATION_ERROR(-20100, L_SQLERRM);
`
`END;"
              
              
              #$loadfile += "==========" + $ExcelTable.PadRight(90).Replace(" ","=") + "`n" + $loadquery + "`n===================================================================================================="
              #$selfile += $ExcelTable.PadLeft(50).PadRight(100).Replace(" ","=")
              #$loadquery > C:\Users\woori\Documents\99.source\LOADSOURCE\LCD_$table"_TG".sql
              $program > C:\Users\woori\Documents\99.source\추출PROGRAM\DW적재\SP_$ExcelTarTable.sql

           } else {

$program  = "CREATE OR REPLACE PROCEDURE CDAPP.SP_" + $ExcelTarTable + "
`  (
`    P_STD_DT      IN VARCHAR2                                                                       /* 기준일(일 작업인 경우 필수 항목) */
`  , P_HIRK_SEQ_INF IN VARCHAR2    DEFAULT NULL                                                      /* 최상위 시퀀스정보 */
`  , P_POSI_SEQ_INF IN VARCHAR2    DEFAULT NULL                                                      /* 바로위 시퀀스정보 */
`  )
`IS
`
`  /*----------------------------------------------------------------------------------------------*/
`  /*      전체 공통 변수 선언                                                                     */
`  /*----------------------------------------------------------------------------------------------*/
`  L_STA_DH             VARCHAR2(14);                                                                /* 시작일시 */
`  L_PGM_ID             VARCHAR2(50);                                                                /* 프로그램ID */
`  L_XTRC_CN            NUMBER(18);                                                                  /* 추출건수 */
`  L_LOAD_CN            NUMBER(18);                                                                  /* 적재건수 */
`  L_DEL_CN             NUMBER(18);                                                                  /* 삭제건수 */
`  L_PARAM_INF          VARCHAR2(50);                                                                /* 파라미터 */
`  L_PRCD_STP_INF       VARCHAR2(50);                                                                /* 작업단계 */
`  L_COMRET             NUMBER(1);                                                                   /* SF_RSDWCOMN 의 RETURN 값 */
`  L_RETURN             NUMBER(1);                                                                   /* SF_WCDCM001TH 의 RETURN 값 */
`  L_SQLERRM            VARCHAR2(500);                                                               /*  오라클 에러메세지 */
`  L_TRC_PARTITION      VARCHAR2(200);                                                               /* PARTITION TRUNCATE OUTPUT PARAMETER */
`                                                                                                 
`  L_LBLA               VARCHAR2(2);                                                                 /* LB/LA */
`  L_STD_YM             VARCHAR2(6);                                                                 /* LB/LA */
`
`
`BEGIN
`
`  /*----------------------------------------------------------------------------------------------*/
`  /*     전체 공통 변수 초기 SETTING                                                              */
`  /*----------------------------------------------------------------------------------------------*/
`  SELECT TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') INTO L_STA_DH FROM DUAL;                               /* 시작일시 */
`  L_PGM_ID        := 'SP_" + ($ExcelTarTable + "';").PadRight(75) + "/* 프로그램ID */
`  L_XTRC_CN       := 0;                                                                             /* 추출건수 */
`  L_LOAD_CN       := 0;                                                                             /* 적재건수 */
`  L_DEL_CN        := 0;                                                                             /* 삭제건수 */
`  L_PARAM_INF     := P_STD_DT;                                                                      /* 파라미터 */
`  L_PRCD_STP_INF := '*';                                                                            /* 작업단계 */
`  L_LBLA         := 'LA';
`  L_STD_YM       := SUBSTR(P_STD_DT,1,6);
`
`  /*----------------------------------------------------------------------------------------------*/
`  /*       등록되지 않은 PROGRAM 인 경우 EXCEPTION 처리 및 종료                                   */
`  /*----------------------------------------------------------------------------------------------*/
`  L_COMRET := SF_RSDWCOMN(L_PGM_ID, P_STD_DT);
`
`  IF L_COMRET <> 0 THEN
`    L_LBLA := 'LB';
`  END IF;
`
`  CASE WHEN L_COMRET = 0
`       THEN NULL;
`       WHEN L_COMRET = 1 THEN
`          RAISE_APPLICATION_ERROR(-20001,'미등록 PGM_ID입니다');
`       WHEN L_COMRET = 2 THEN
`          RAISE_APPLICATION_ERROR(-20002,'선행프로그램이 미실행입니다');
`       WHEN L_COMRET = 3 THEN
`          RAISE_APPLICATION_ERROR(-20003,'동일기간 중복 실행입니다');
`  END CASE;
`
`  /*----------------------------------------------------------------------------------------------*/
`  /*       시작 LOG SETTING/시작시간 확인                                                         */
`  /*----------------------------------------------------------------------------------------------*/
`  IF SF_WCDCM901TH ('LB' ,NULL  ,L_PGM_ID ,L_PARAM_INF,L_STA_DH ,L_PRCD_STP_INF,'RUNNING', L_XTRC_CN, L_LOAD_CN ,L_DEL_CN ,L_PARAM_INF ,P_HIRK_SEQ_INF ,P_POSI_SEQ_INF ) != 0 THEN
`     NULL;
`  END IF;
`
`    DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 작업시작 = [' || L_STA_DH || ']');
`    DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 기준년월 = [' || P_STD_DT || ']');
`    DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 변환구분 = [' || P_HIRK_SEQ_INF || ']');
`
`  /*----------------------------------------------------------------------------------------------*/
`  /*             BIZ LOGIC 처리 시작                                                              */
`  /*----------------------------------------------------------------------------------------------*/
`
`  /*----------------------------------------------------------------------------------------------*/
`  /* STEP01 : TEMP TABLE -> TARTGET TABLE JOIN DELETE                                             */
`  /*----------------------------------------------------------------------------------------------*/
`  L_PRCD_STP_INF := '01';
`  L_RETURN := SF_WCDCM901TH ('LU' ,NULL  ,L_PGM_ID ,L_PARAM_INF,L_STA_DH ,L_PRCD_STP_INF);
`  DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 작업 STEP = [' || L_PRCD_STP_INF || ']['|| TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS')||']');
`
`    DELETE /*+ LEADING(B A) USE_HASH(B A) PARALLEL(A 32) PARALLEL(B 32) */
`      FROM " + $ExcelTarTable + " A
`     WHERE EXISTS (
`           SELECT /*+ PARALLEL(B 4) FULL(B) */ 1
`             FROM CDTMP." + $ExcelTarTable + "_T01 B
$colPkArr        )
`   ;
`
`	 L_DEL_CN := SQL%ROWCOUNT;
`	 DBMS_OUTPUT.PUT_LINE('삭제건수 =[' || L_DEL_CN || ']');
`
`COMMIT;
`
`  /*----------------------------------------------------------------------------------------------*/
`  /* STEP02 : TEMP TABLE -> TARTGET TABLE INSERT                                                  */
`  /*----------------------------------------------------------------------------------------------*/
`  L_PRCD_STP_INF := '02';
`  L_RETURN := SF_WCDCM901TH ('LU' ,NULL  ,L_PGM_ID ,L_PARAM_INF,L_STA_DH ,L_PRCD_STP_INF);
`  DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 작업 STEP = [' || L_PRCD_STP_INF || ']['|| TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS')||']');
`
`
`  	 INSERT /*+ APPEND PARALLEL(Z 8) */ INTO CDOWN." + $ExcelTarTable + "
`          (
$colArr          )
`  	 SELECT /*+ LEADING(A) PARALLEL(A 8) FULL(A) USE_HASH(A) */
$colArrTemp  	   FROM CDTMP." + $ExcelTarTable + "_T01
`             ;
`
`	L_LOAD_CN := SQL%ROWCOUNT;
`	DBMS_OUTPUT.PUT_LINE('적재건수 =[' || L_LOAD_CN || ']');
`
`  COMMIT;
`
`  /*----------------------------------------------------------------------------------------------*/
`  /*       종료 LOG SETTING/종료시간 확인                                                         */
`  /*----------------------------------------------------------------------------------------------*/
`  L_RETURN := SF_WCDCM901TH ('LA' ,NULL ,L_PGM_ID ,L_PARAM_INF,L_STA_DH ,L_PRCD_STP_INF,'Finished', L_XTRC_CN, L_LOAD_CN ,L_DEL_CN ,L_PARAM_INF ,P_HIRK_SEQ_INF ,P_POSI_SEQ_INF ) ;
`  DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 작업종료 = [' || TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') || ']');
`  DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 기준년월 = [' || P_STD_DT || ']');
`
`EXCEPTION
`  /* 예외발생 :  ABORTED로 종료 */
`  WHEN OTHERS THEN
`       L_SQLERRM := SQLERRM ;
`       DBMS_OUTPUT.PUT_LINE('L_SQLERRM='||L_SQLERRM);
`       L_RETURN := SF_WCDCM901TH (L_LBLA, NULL ,L_PGM_ID ,P_STD_DT,L_STA_DH ,L_PRCD_STP_INF,'Aborted', L_LOAD_CN, L_LOAD_CN ,L_DEL_CN ,'P_STD_DT='||P_STD_DT ,P_HIRK_SEQ_INF ,P_POSI_SEQ_INF ,L_SQLERRM) ;
`       DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : L_PRCD_STP_INF = [' || L_PRCD_STP_INF || ']');
`       DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : EXCEPTION = [기타], ERR_CODE=[' || TO_CHAR(SQLCODE) || ']');
`       DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : EXCEPTION = [기타], ERR_MSG= [' || L_SQLERRM        || ']');
`       ROLLBACK;
`       --RAISE_APPLICATION_ERROR(-20100, L_SQLERRM);
`
`END;"
              
              
              #$loadfile += "==========" + $ExcelTable.PadRight(90).Replace(" ","=") + "`n" + $loadquery + "`n===================================================================================================="
              #$selfile += $ExcelTable.PadLeft(50).PadRight(100).Replace(" ","=")
              #$loadquery > C:\Users\woori\Documents\99.source\LOADSOURCE\LCD_$table"_TG".sql
              $program > C:\Users\woori\Documents\99.source\추출PROGRAM\DW적재\SP_$ExcelTarTable.sql
           }
         }
     }
     #$selfile > C:\Users\woori\Documents\99.source\selectALL.sql
     #$crefile > C:\Users\woori\Documents\99.source\createALL.sql
     #$loadfile > C:\Users\woori\Documents\99.source\loadALL.sql
     $ExcelWorkBook.Close()
     #$con.close()
     Write-Host "=================================================================================================="
     Write-Host "=============================================작업완료============================================="
     Write-Host "=================================================================================================="
} catch {
     Write-Error ("Database Exception:{0}`n{1}" -f ` $con.ConnectionString, $_.Exception.ToString())
} finally{
     if ($con.State -eq 'Open') {
        $con.close() 
     }
}