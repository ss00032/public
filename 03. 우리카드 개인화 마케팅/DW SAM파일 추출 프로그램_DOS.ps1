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
     $ExcelWorkSheet = $ExcelWorkBook.Sheets.Item("파워쉘작업_DW추출")
     $rowcount=$ExcelWorkSheet.UsedRange.Rows.Count

     $selfile=@()
     $crefile=@()
     $loadfile=@()

     for ($i=2; $i -le $rowcount; $i++) {
         $ExcelOwner = $ExcelWorkSheet.cells.Item($i, 5).value2           #원천 OWNER
         $ExcelTable = $ExcelWorkSheet.cells.Item($i, 6).value2           #원천 TABLE
         $ExcelTableNm = $ExcelWorkSheet.cells.Item($i, 7).value2         #원천 TABLE명
         $ExcelTarTable = $ExcelWorkSheet.cells.Item($i, 8).value2        #타겟 TABLE
         $ExcelParTable = $ExcelWorkSheet.cells.Item($i, 9).value2        #파티션 키
         Write-Host "--------------------" $ExcelOwner"."$ExcelTarTable
         #$statement = "select distinct owner, table_name, column_name, column_id from all_tab_columns where table_name = '" + $ExcelData + "' order by column_id"
         $statement = "SELECT
                      `       A.OWNER
                      `     , A.TABLE_NAME
                      `     , nvl(B.COMMENTS,'없음')       AS TABLE_COMMENTS
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
                      ` WHERE A.OWNER = '" + $ExcelOwner + "'
                      `   AND A.TABLE_NAME = '" + $ExcelTable + "'
                      `   AND A.COLUMN_NAME NOT LIKE '%PSWD'
                      `   AND A.HIDDEN_COLUMN = 'NO'
                      ` ORDER BY A.OWNER
                      `        , A.TABLE_NAME
                      `        , A.COLUMN_ID"
         $con = New-Object System.Data.OracleClient.OracleConnection($dw_connection_string)
         $con.Open()

         $cmd = $con.CreateCommand()
         $cmd.CommandText = $statement

         $result = $cmd.ExecuteReader()
         $colArr=@()
         $colDataArr=@()
         $colloadArr=@()

         while ($result.Read())
         {
          $Owner = $result.GetString(0)
          $table = $result.GetString(1)
          $tablenm = $result.GetString(2)
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

          #컬럼 레이아웃 셋팅
          if ($ExcelTable -eq 'WCMGE901TM' -and ($column -eq 'RNM_NO' -or $column -eq 'CI_NO')) {
            continue
          } elseif ($col_id -eq 1) {
            $colArr += "           A." + $column.PadRight(87) + "/* " + $columnnm + " */`n"
          }else {
            $colArr += "        , A." + $column.PadRight(87) + "/* " + $columnnm + " */`n"
          }

          #CURSOR 로직 셋팅
          if ($ExcelTable -eq 'WCMGE901TM' -and ($column -eq 'RNM_NO' -or $column -eq 'CI_NO')) {
            continue
          } elseif($col_id -eq 1) {
            $colDataArr += $ExcelTable + "_REC." + $column.PadRight(65) + "/* " + $columnnm + " */`n"
          }else {
            $colDataArr += "       ||'|^,|'||     " + $ExcelTable + "_REC." + $column.PadRight(65) + "/* " + $columnnm + " */`n"
          }
         }

         #007 월작업인데 15일자 데이터 받기위해 방어로직 추가 (L_P_STD_DT)
         if ($ExcelTarTable -eq 'WCPSR007TH(사용안하게됨)') {
$program  = "CREATE OR REPLACE PROCEDURE CDAPP.SP_" + $ExcelTarTable + "
`(
`    P_STD_DT      IN VARCHAR2                                                                       /* 기준년월일(월 작업인 경우 필수 항목) */
`  , P_HIRK_SEQ_INF IN VARCHAR2    DEFAULT NULL                                                      /* 최상위 시퀀스정보 */
`  , P_POSI_SEQ_INF IN VARCHAR2    DEFAULT NULL                                                      /* 바로위 시퀀스정보 */
`) IS
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*      전체 공통 변수 선언                                                                   */
`    /*--------------------------------------------------------------------------------------------*/
`
`    L_STA_DH             VARCHAR2(14)  := TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS');                      /* 시작일시 */ 
`    L_PGM_ID            VARCHAR2(200)  := 'SP_" + $ExcelTarTable + "';".PadRight(44) +               "/* 프로그램ID */
`    L_P_STD_DT          VARCHAR2(8)    := P_STD_DT || '15';
`    L_FILE_ID           VARCHAR2(50)   := '" + $ExcelTarTable + "'" + "||'_'||  L_P_STD_DT ||'.dat';".PadRight(46) +     "/* 파일ID */
`    L_XTRC_CN            NUMBER(18)    := 0;                                                        /* 추출건수 */
`    L_LOAD_CN            NUMBER(18)    := 0;                                                        /* 적재건수 */
`    L_DEL_CN             NUMBER(18)    := 0;                                                        /* 삭제건수 */
`    L_PARAM_INF          VARCHAR2(50)  := NULL;                                                     /* 파라미터 */
`    L_PRCD_STP_INF       VARCHAR2(50)  := NULL;                                                     /* 작업단계 */
`    L_LBLA               VARCHAR2(2);                                                               /* LB/LA */
`    L_COMRET             NUMBER(1)     := NULL;                                                     /* SF_RsDWComn 의 RETURN 값 */
`    L_RETURN             NUMBER(1)     := NULL;                                                     /* SF_WCDCM901TH 의 RETURN 값 */
`    L_SQLERRM            VARCHAR2(500) := NULL;                                                     /* 오라클 에러메세지 */
`    L_RBS_CN             NUMBER(18)    := 0;
`
`    NOT_REGISTER_PGM_ID  EXCEPTION;                                                                 /* 미등록 프로그램 Exception */
`    NOT_BF_PGM_ID        EXCEPTION;                                                                 /* 선행job 미실행  Exception */
`    RUNNING_PGM_ID       EXCEPTION;                                                                 /* 미등록 프로그램 Exception */
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*      프로그램내 변수 선언                                                                  */
`    /*--------------------------------------------------------------------------------------------*/
`    L_STD_YMD           VARCHAR2(8)    := P_STD_DT;
`    L_BAS_DT            VARCHAR2(8)    := NULL;
`    L_5BIZ_DT           VARCHAR2(8)    := NULL;
`    L_BF_HY_END_DT      VARCHAR2(8)    := NULL;
`    L_MAX_DB_RGS_DH     VARCHAR2(16)   := NULL;
`    DIRLOC              VARCHAR2(300)  := NULL;
`    SAM_FILE            VARCHAR2(4000) := NULL;
`    SAMFILE_W           UTL_FILE.FILE_TYPE;
`
`    V_CNT               NUMBER := 0;
`    V_SUM               NUMBER := 0;
`    L_EX_CN             NUMBER := 0;
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*   추출 DATA  CURSOR                                                                        */ 
`    /*--------------------------------------------------------------------------------------------*/
`    CURSOR " + $ExcelTable + "_CUR IS
`    SELECT /*+ PARALLEL(A 16) FULL(A) */
$colArr         , 'SP_" + ($ExcelTarTable + "' AS MKT_LOAD_PGM_ID").PadRight(85) + "/* 마케팅적재프로그램ID */
`         , TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') AS MKT_LOAD_DH".PadRight(119) + "/* 마케팅적재일시 */ 
`      FROM " + $ExcelOwner + "."+ ($ExcelTable + " A").PadRight(83) + "/* " + $tablenm + " */
`     WHERE A." + $ExcelParTable + " = L_P_STD_DT
`    ;
`
`BEGIN
`    /*--------------------------------------------------------------------------------------------*/
`    /*     전체 공통 변수 초기 Setting                                                            */
`    /*--------------------------------------------------------------------------------------------*/
`    L_XTRC_CN       := 0;                                                                           /* 추출건수 */
`    L_LOAD_CN       := 0;                                                                           /* 적재건수 */
`    L_DEL_CN        := 0;                                                                           /* 삭제건수 */
`    L_PARAM_INF     := L_STD_YMD;                                                                   /* 파라미터 */
`    L_PRCD_STP_INF  := '';                                                                          /* 작업단계 */
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*         프로그램내 변수 초기 Setting                                                       */
`    /*--------------------------------------------------------------------------------------------*/
`    L_PRCD_STP_INF := '*';
`    L_LBLA         := 'LA';
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*       등록되지 않은 PROGRAM 인 경우 Exception 처리 및 종료                                 */
`    /*--------------------------------------------------------------------------------------------*/
`    L_COMRET := SF_RsDWComn(L_PGM_ID, L_P_STD_DT);
`
`    IF L_COMRET <> 0 THEN
`       L_LBLA := 'LB';
`    END IF;
`
`    CASE WHEN L_COMRET = 0
`              THEN NULL;
`         WHEN L_COMRET = 1 THEN
`              RAISE_APPLICATION_ERROR(-20001,'미등록 PGM_ID입니다');
`         WHEN L_COMRET = 2 THEN
`              RAISE_APPLICATION_ERROR(-20002,'선행프로그램이 미실행입니다');
`         WHEN L_COMRET = 3 THEN
`              RAISE_APPLICATION_ERROR(-20003,'동일기간 중복 실행입니다');
`    END CASE;
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*       시작 LOG Setting/시작시간 확인                                                       */
`    /*--------------------------------------------------------------------------------------------*/
`    IF SF_WCDCM901TH ( 'LB', NULL, L_PGM_ID, L_PARAM_INF, L_STA_DH, L_PRCD_STP_INF, 'Running', L_XTRC_CN, L_LOAD_CN, L_DEL_CN, L_PARAM_INF
`                      ,P_HIRK_SEQ_INF, P_POSI_SEQ_INF) != 0 THEN
`        NULL;
`    END IF;
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*   적재 대상 Sam File Open                                                                  */
`    /*   - 존재하면 overwrite else create                                                         */
`    /*--------------------------------------------------------------------------------------------*/
`    /* 프로그램에 해당하는 디렉토리를 구한다 */
`    DIRLOC := SF_GetIfOutPath(L_PGM_ID);
`
`    IF DIRLOC = '1403' THEN
`       RAISE_APPLICATION_ERROR(-20004,'프로그램에 해당하는 디렉토리가 존재하지 않습니다');
`    ELSIF DIRLOC = '9999' THEN
`       RAISE_APPLICATION_ERROR(-20005,'디렉토리 검색 에러 입니다');
`    END IF;
`
`    SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',4000);                                       /* BUFFER MAX(6400) */
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*   Data write : 로직으로 발생된 데이터를 Put_line을 이용해 Write                            */
`    /*   - Put_line : 하나의 row단위로 write --(표준)                                             */
`    /*   - Putf    : 변수와 값을 동시에 간단하게 작성할경우 사용                                  */
`    /*--------------------------------------------------------------------------------------------*/
`
`    FOR " + $ExcelTable + "_REC IN " + $ExcelTable + "_CUR LOOP
`
`        SAM_FILE :=    $colDataArr        ||'|^,|'||     " + ($ExcelTable + "_REC.MKT_LOAD_PGM_ID").PadRight(80) + "/* 마케팅적재프로그램ID */
`        ||'|^,|'||     " + ($ExcelTable + "_REC.MKT_LOAD_DH").PadRight(80) + "/* 마케팅적재일시 */
`        ||'|^,|'
`        ;
`        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);
`
`        V_CNT := V_CNT + 1;
`
`    END LOOP;
`         
`
`    DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 추출건수   = [' || V_CNT || ']');
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*   적재 대상 Sam File close                                                                 */
`    /*--------------------------------------------------------------------------------------------*/
`    UTL_FILE.FCLOSE(SAMFILE_W);
`
`    L_XTRC_CN := V_CNT;
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*       종료 LOG Setting/종료시간 확인                                                       */
`    /*--------------------------------------------------------------------------------------------*/
`    L_RETURN := SF_WCDCM901TH ( 'LA', NULL, L_PGM_ID, L_PARAM_INF, L_STA_DH, L_PRCD_STP_INF, 'Finished', L_XTRC_CN, L_LOAD_CN, L_DEL_CN, L_PARAM_INF, P_HIRK_SEQ_INF, P_POSI_SEQ_INF );
`
`    DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 작업종료 = [' || TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS')  || ']');
`    DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 기준일자 = [' || L_P_STD_DT || ']');
`
`EXCEPTION
`    /*--------------------------------------------------------------------------------------------*/
`    /*   SP 표준  EXCEPTION CHECK !!!                                                             */
`    /*--------------------------------------------------------------------------------------------*/
`    WHEN OTHERS THEN
`       L_SQLERRM := SQLERRM;
`       DBMS_OUTPUT.PUT_LINE('L_SQLERRM='||L_SQLERRM);
`       L_RETURN := SF_WCDCM901TH (L_LBLA, NULL ,L_PGM_ID ,L_PARAM_INF,L_STA_DH ,L_PRCD_STP_INF,'Aborted', L_XTRC_CN, L_LOAD_CN ,L_DEL_CN ,L_PARAM_INF ,P_HIRK_SEQ_INF ,P_POSI_SEQ_INF ,L_SQLERRM) ;
`       DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : Exception = [기타], ERR_CODE=[' || TO_CHAR(SQLCODE) || ']');
`       DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : Exception = [기타], ERR_MSG= [' || L_SQLERRM        || ']');
`       ROLLBACK;
`       RAISE_APPLICATION_ERROR(-20100, L_SQLERRM);
`END;"
    
             #$loadfile += "==========" + $ExcelTable.PadRight(90).Replace(" ","=") + "`n" + $loadquery + "`n===================================================================================================="
             #$selfile += $ExcelTable.PadLeft(50).PadRight(100).Replace(" ","=")
             #$loadquery > C:\Users\woori\Documents\99.source\LOADSOURCE\LCD_$table"_TG".sql
             $program > C:\Users\woori\Documents\99.source\추출PROGRAM\DW추출\SP_$ExcelTarTable.sql
         
         #일자 파티션인 테이블 생성 로직
         } elseif ($ExcelParTable -ne 'WFG_CD') {
$program  = "CREATE OR REPLACE PROCEDURE CDAPP.SP_" + $ExcelTarTable + "
`(
`    P_STD_DT      IN VARCHAR2                                                                       /* 기준년월일(월 작업인 경우 필수 항목) */
`  , P_HIRK_SEQ_INF IN VARCHAR2    DEFAULT NULL                                                      /* 최상위 시퀀스정보 */
`  , P_POSI_SEQ_INF IN VARCHAR2    DEFAULT NULL                                                      /* 바로위 시퀀스정보 */
`) IS
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*      전체 공통 변수 선언                                                                   */
`    /*--------------------------------------------------------------------------------------------*/
`
`    L_STA_DH             VARCHAR2(14)  := TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS');                      /* 시작일시 */ 
`    L_PGM_ID            VARCHAR2(200)  := 'SP_" + $ExcelTarTable + "';".PadRight(44) +               "/* 프로그램ID */
`    L_FILE_ID           VARCHAR2(50)   := '" + $ExcelTarTable + "'" + "||'_'||  P_STD_DT ||'.dat';".PadRight(46) +     "/* 파일ID */
`    L_XTRC_CN            NUMBER(18)    := 0;                                                        /* 추출건수 */
`    L_LOAD_CN            NUMBER(18)    := 0;                                                        /* 적재건수 */
`    L_DEL_CN             NUMBER(18)    := 0;                                                        /* 삭제건수 */
`    L_PARAM_INF          VARCHAR2(50)  := NULL;                                                     /* 파라미터 */
`    L_PRCD_STP_INF       VARCHAR2(50)  := NULL;                                                     /* 작업단계 */
`    L_LBLA               VARCHAR2(2);                                                               /* LB/LA */
`    L_COMRET             NUMBER(1)     := NULL;                                                     /* SF_RsDWComn 의 RETURN 값 */
`    L_RETURN             NUMBER(1)     := NULL;                                                     /* SF_WCDCM901TH 의 RETURN 값 */
`    L_SQLERRM            VARCHAR2(500) := NULL;                                                     /* 오라클 에러메세지 */
`    L_RBS_CN             NUMBER(18)    := 0;
`
`    NOT_REGISTER_PGM_ID  EXCEPTION;                                                                 /* 미등록 프로그램 Exception */
`    NOT_BF_PGM_ID        EXCEPTION;                                                                 /* 선행job 미실행  Exception */
`    RUNNING_PGM_ID       EXCEPTION;                                                                 /* 미등록 프로그램 Exception */
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*      프로그램내 변수 선언                                                                  */
`    /*--------------------------------------------------------------------------------------------*/
`    L_STD_YMD           VARCHAR2(8)    := P_STD_DT;
`    L_BAS_DT            VARCHAR2(8)    := NULL;
`    L_5BIZ_DT           VARCHAR2(8)    := NULL;
`    L_BF_HY_END_DT      VARCHAR2(8)    := NULL;
`    L_MAX_DB_RGS_DH     VARCHAR2(16)   := NULL;
`    DIRLOC              VARCHAR2(300)  := NULL;
`    SAM_FILE            VARCHAR2(4000) := NULL;
`    SAMFILE_W           UTL_FILE.FILE_TYPE;
`
`    V_CNT               NUMBER := 0;
`    V_SUM               NUMBER := 0;
`    L_EX_CN             NUMBER := 0;
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*   추출 DATA  CURSOR                                                                        */ 
`    /*--------------------------------------------------------------------------------------------*/
`    CURSOR " + $ExcelTable + "_CUR IS
`    SELECT /*+ PARALLEL(A 16) FULL(A) */
$colArr         , 'SP_" + ($ExcelTarTable + "' AS MKT_LOAD_PGM_ID").PadRight(85) + "/* 마케팅적재프로그램ID */
`         , TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') AS MKT_LOAD_DH".PadRight(119) + "/* 마케팅적재일시 */ 
`      FROM " + $ExcelOwner + "."+ ($ExcelTable + " A").PadRight(83) + "/* " + $tablenm + " */
`     WHERE A." + $ExcelParTable + " = P_STD_DT
`    ;
`
`BEGIN
`    /*--------------------------------------------------------------------------------------------*/
`    /*     전체 공통 변수 초기 Setting                                                            */
`    /*--------------------------------------------------------------------------------------------*/
`    L_XTRC_CN       := 0;                                                                           /* 추출건수 */
`    L_LOAD_CN       := 0;                                                                           /* 적재건수 */
`    L_DEL_CN        := 0;                                                                           /* 삭제건수 */
`    L_PARAM_INF     := L_STD_YMD;                                                                   /* 파라미터 */
`    L_PRCD_STP_INF  := '';                                                                          /* 작업단계 */
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*         프로그램내 변수 초기 Setting                                                       */
`    /*--------------------------------------------------------------------------------------------*/
`    L_PRCD_STP_INF := '*';
`    L_LBLA         := 'LA';
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*       등록되지 않은 PROGRAM 인 경우 Exception 처리 및 종료                                 */
`    /*--------------------------------------------------------------------------------------------*/
`    L_COMRET := SF_RsDWComn(L_PGM_ID, P_STD_DT);
`
`    IF L_COMRET <> 0 THEN
`       L_LBLA := 'LB';
`    END IF;
`
`    CASE WHEN L_COMRET = 0
`              THEN NULL;
`         WHEN L_COMRET = 1 THEN
`              RAISE_APPLICATION_ERROR(-20001,'미등록 PGM_ID입니다');
`         WHEN L_COMRET = 2 THEN
`              RAISE_APPLICATION_ERROR(-20002,'선행프로그램이 미실행입니다');
`         WHEN L_COMRET = 3 THEN
`              RAISE_APPLICATION_ERROR(-20003,'동일기간 중복 실행입니다');
`    END CASE;
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*       시작 LOG Setting/시작시간 확인                                                       */
`    /*--------------------------------------------------------------------------------------------*/
`    IF SF_WCDCM901TH ( 'LB', NULL, L_PGM_ID, L_PARAM_INF, L_STA_DH, L_PRCD_STP_INF, 'Running', L_XTRC_CN, L_LOAD_CN, L_DEL_CN, L_PARAM_INF
`                      ,P_HIRK_SEQ_INF, P_POSI_SEQ_INF) != 0 THEN
`        NULL;
`    END IF;
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*   적재 대상 Sam File Open                                                                  */
`    /*   - 존재하면 overwrite else create                                                         */
`    /*--------------------------------------------------------------------------------------------*/
`    /* 프로그램에 해당하는 디렉토리를 구한다 */
`    DIRLOC := SF_GetIfOutPath(L_PGM_ID);
`
`    IF DIRLOC = '1403' THEN
`       RAISE_APPLICATION_ERROR(-20004,'프로그램에 해당하는 디렉토리가 존재하지 않습니다');
`    ELSIF DIRLOC = '9999' THEN
`       RAISE_APPLICATION_ERROR(-20005,'디렉토리 검색 에러 입니다');
`    END IF;
`
`    SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',4000);                                       /* BUFFER MAX(6400) */
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*   Data write : 로직으로 발생된 데이터를 Put_line을 이용해 Write                            */
`    /*   - Put_line : 하나의 row단위로 write --(표준)                                             */
`    /*   - Putf    : 변수와 값을 동시에 간단하게 작성할경우 사용                                  */
`    /*--------------------------------------------------------------------------------------------*/
`
`    FOR " + $ExcelTable + "_REC IN " + $ExcelTable + "_CUR LOOP
`
`        SAM_FILE :=    $colDataArr        ||'|^,|'||     " + ($ExcelTable + "_REC.MKT_LOAD_PGM_ID").PadRight(80) + "/* 마케팅적재프로그램ID */
`        ||'|^,|'||     " + ($ExcelTable + "_REC.MKT_LOAD_DH").PadRight(80) + "/* 마케팅적재일시 */
`        ||'|^,|'
`        ;
`        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);
`
`        V_CNT := V_CNT + 1;
`
`    END LOOP;
`         
`
`    DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 추출건수   = [' || V_CNT || ']');
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*   적재 대상 Sam File close                                                                 */
`    /*--------------------------------------------------------------------------------------------*/
`    UTL_FILE.FCLOSE(SAMFILE_W);
`
`    L_XTRC_CN := V_CNT;
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*       종료 LOG Setting/종료시간 확인                                                       */
`    /*--------------------------------------------------------------------------------------------*/
`    L_RETURN := SF_WCDCM901TH ( 'LA', NULL, L_PGM_ID, L_PARAM_INF, L_STA_DH, L_PRCD_STP_INF, 'Finished', L_XTRC_CN, L_LOAD_CN, L_DEL_CN, L_PARAM_INF, P_HIRK_SEQ_INF, P_POSI_SEQ_INF );
`
`    DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 작업종료 = [' || TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS')  || ']');
`    DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 기준일자 = [' || P_STD_DT || ']');
`
`EXCEPTION
`    /*--------------------------------------------------------------------------------------------*/
`    /*   SP 표준  EXCEPTION CHECK !!!                                                             */
`    /*--------------------------------------------------------------------------------------------*/
`    WHEN OTHERS THEN
`       L_SQLERRM := SQLERRM;
`       DBMS_OUTPUT.PUT_LINE('L_SQLERRM='||L_SQLERRM);
`       L_RETURN := SF_WCDCM901TH (L_LBLA, NULL ,L_PGM_ID ,L_PARAM_INF,L_STA_DH ,L_PRCD_STP_INF,'Aborted', L_XTRC_CN, L_LOAD_CN ,L_DEL_CN ,L_PARAM_INF ,P_HIRK_SEQ_INF ,P_POSI_SEQ_INF ,L_SQLERRM) ;
`       DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : Exception = [기타], ERR_CODE=[' || TO_CHAR(SQLCODE) || ']');
`       DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : Exception = [기타], ERR_MSG= [' || L_SQLERRM        || ']');
`       ROLLBACK;
`       RAISE_APPLICATION_ERROR(-20100, L_SQLERRM);
`END;"
    
             #$loadfile += "==========" + $ExcelTable.PadRight(90).Replace(" ","=") + "`n" + $loadquery + "`n===================================================================================================="
             #$selfile += $ExcelTable.PadLeft(50).PadRight(100).Replace(" ","=")
             #$loadquery > C:\Users\woori\Documents\99.source\LOADSOURCE\LCD_$table"_TG".sql
             $program > C:\Users\woori\Documents\99.source\추출PROGRAM\DW추출\SP_$ExcelTarTable.sql
         } else {
$program  = "CREATE OR REPLACE PROCEDURE CDAPP.SP_" + $ExcelTarTable + "
`(
`    P_STD_DT      IN VARCHAR2                                                                       /* 기준년월일(월 작업인 경우 필수 항목) */
`  , P_HIRK_SEQ_INF IN VARCHAR2    DEFAULT NULL                                                      /* 최상위 시퀀스정보 */
`  , P_POSI_SEQ_INF IN VARCHAR2    DEFAULT NULL                                                      /* 바로위 시퀀스정보 */
`) IS
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*      전체 공통 변수 선언                                                                   */
`    /*--------------------------------------------------------------------------------------------*/
`
`    L_STA_DH             VARCHAR2(14)  := TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS');                      /* 시작일시 */ 
`    L_PGM_ID            VARCHAR2(200)  := 'SP_" + $ExcelTarTable + "';".PadRight(44) +               "/* 프로그램ID */
`    L_FILE_ID           VARCHAR2(50)   := '" + $ExcelTarTable + "'" + "||'_'||  P_STD_DT ||'.dat';".PadRight(46) +     "/* 파일ID */
`    L_XTRC_CN            NUMBER(18)    := 0;                                                        /* 추출건수 */
`    L_LOAD_CN            NUMBER(18)    := 0;                                                        /* 적재건수 */
`    L_DEL_CN             NUMBER(18)    := 0;                                                        /* 삭제건수 */
`    L_PARAM_INF          VARCHAR2(50)  := NULL;                                                     /* 파라미터 */
`    L_PRCD_STP_INF       VARCHAR2(50)  := NULL;                                                     /* 작업단계 */
`    L_LBLA               VARCHAR2(2);                                                               /* LB/LA */
`    L_COMRET             NUMBER(1)     := NULL;                                                     /* SF_RsDWComn 의 RETURN 값 */
`    L_RETURN             NUMBER(1)     := NULL;                                                     /* SF_WCDCM901TH 의 RETURN 값 */
`    L_SQLERRM            VARCHAR2(500) := NULL;                                                     /* 오라클 에러메세지 */
`    L_RBS_CN             NUMBER(18)    := 0;
`
`    NOT_REGISTER_PGM_ID  EXCEPTION;                                                                 /* 미등록 프로그램 Exception */
`    NOT_BF_PGM_ID        EXCEPTION;                                                                 /* 선행job 미실행  Exception */
`    RUNNING_PGM_ID       EXCEPTION;                                                                 /* 미등록 프로그램 Exception */
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*      프로그램내 변수 선언                                                                  */
`    /*--------------------------------------------------------------------------------------------*/
`    L_STD_YMD           VARCHAR2(8)    := P_STD_DT;
`    L_BAS_DT            VARCHAR2(8)    := NULL;
`    L_5BIZ_DT           VARCHAR2(8)    := NULL;
`    L_BF_HY_END_DT      VARCHAR2(8)    := NULL;
`    L_MAX_DB_RGS_DH     VARCHAR2(16)   := NULL;
`    DIRLOC              VARCHAR2(300)  := NULL;
`    SAM_FILE            VARCHAR2(4000) := NULL;
`    SAMFILE_W           UTL_FILE.FILE_TYPE;
`
`    V_CNT               NUMBER := 0;
`    V_SUM               NUMBER := 0;
`    L_EX_CN             NUMBER := 0;
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*   추출 DATA  CURSOR                                                                        */ 
`    /*--------------------------------------------------------------------------------------------*/
`    CURSOR " + $ExcelTable + "_CUR IS
`    SELECT /*+ PARALLEL(A 16) FULL(A) */
$colArr         , 'SP_" + ($ExcelTarTable + "' AS MKT_LOAD_PGM_ID").PadRight(85) + "/* 마케팅적재프로그램ID */
`         , TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') AS MKT_LOAD_DH".PadRight(119) + "/* 마케팅적재일시 */ 
`      FROM " + $ExcelOwner + "."+ ($ExcelTable + " A").PadRight(83) + "/* " + $tablenm + " */
`    ;
`
`BEGIN
`    /*--------------------------------------------------------------------------------------------*/
`    /*     전체 공통 변수 초기 Setting                                                            */
`    /*--------------------------------------------------------------------------------------------*/
`    L_XTRC_CN       := 0;                                                                           /* 추출건수 */
`    L_LOAD_CN       := 0;                                                                           /* 적재건수 */
`    L_DEL_CN        := 0;                                                                           /* 삭제건수 */
`    L_PARAM_INF     := L_STD_YMD;                                                                   /* 파라미터 */
`    L_PRCD_STP_INF  := '';                                                                          /* 작업단계 */
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*         프로그램내 변수 초기 Setting                                                       */
`    /*--------------------------------------------------------------------------------------------*/
`    L_PRCD_STP_INF := '*';
`    L_LBLA         := 'LA';
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*       등록되지 않은 PROGRAM 인 경우 Exception 처리 및 종료                                 */
`    /*--------------------------------------------------------------------------------------------*/
`    L_COMRET := SF_RsDWComn(L_PGM_ID, P_STD_DT);
`
`    IF L_COMRET <> 0 THEN
`       L_LBLA := 'LB';
`    END IF;
`
`    CASE WHEN L_COMRET = 0
`              THEN NULL;
`         WHEN L_COMRET = 1 THEN
`              RAISE_APPLICATION_ERROR(-20001,'미등록 PGM_ID입니다');
`         WHEN L_COMRET = 2 THEN
`              RAISE_APPLICATION_ERROR(-20002,'선행프로그램이 미실행입니다');
`         WHEN L_COMRET = 3 THEN
`              RAISE_APPLICATION_ERROR(-20003,'동일기간 중복 실행입니다');
`    END CASE;
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*       시작 LOG Setting/시작시간 확인                                                       */
`    /*--------------------------------------------------------------------------------------------*/
`    IF SF_WCDCM901TH ( 'LB', NULL, L_PGM_ID, L_PARAM_INF, L_STA_DH, L_PRCD_STP_INF, 'Running', L_XTRC_CN, L_LOAD_CN, L_DEL_CN, L_PARAM_INF, P_HIRK_SEQ_INF, P_POSI_SEQ_INF) != 0 THEN
`        NULL;
`    END IF;
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*   적재 대상 Sam File Open                                                                  */
`    /*   - 존재하면 overwrite else create                                                         */
`    /*--------------------------------------------------------------------------------------------*/
`    /* 프로그램에 해당하는 디렉토리를 구한다 */
`    DIRLOC := SF_GetIfOutPath(L_PGM_ID);
`
`    IF DIRLOC = '1403' THEN
`       RAISE_APPLICATION_ERROR(-20004,'프로그램에 해당하는 디렉토리가 존재하지 않습니다');
`    ELSIF DIRLOC = '9999' THEN
`       RAISE_APPLICATION_ERROR(-20005,'디렉토리 검색 에러 입니다');
`    END IF;
`
`    SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',4000);                                       /* BUFFER MAX(6400) */
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*   Data write : 로직으로 발생된 데이터를 Put_line을 이용해 Write                            */
`    /*   - Put_line : 하나의 row단위로 write --(표준)                                             */
`    /*   - Putf    : 변수와 값을 동시에 간단하게 작성할경우 사용                                  */
`    /*--------------------------------------------------------------------------------------------*/
`
`    FOR " + $ExcelTable + "_REC IN " + $ExcelTable + "_CUR LOOP
`
`        SAM_FILE :=    $colDataArr        ||'|^,|'||     " + ($ExcelTable + "_REC.MKT_LOAD_PGM_ID").PadRight(80) + "/* 마케팅적재프로그램ID */
`        ||'|^,|'||     " + ($ExcelTable + "_REC.MKT_LOAD_DH").PadRight(80) + "/* 마케팅적재일시 */
`        ||'|^,|'
`        ;
`        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);
`
`        V_CNT := V_CNT + 1;
`
`    END LOOP;
`         
`
`    DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 추출건수   = [' || V_CNT || ']');
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*   적재 대상 Sam File close                                                                 */
`    /*--------------------------------------------------------------------------------------------*/
`    UTL_FILE.FCLOSE(SAMFILE_W);
`
`    L_XTRC_CN := V_CNT;
`
`    /*--------------------------------------------------------------------------------------------*/
`    /*       종료 LOG Setting/종료시간 확인                                                       */
`    /*--------------------------------------------------------------------------------------------*/
`    L_RETURN := SF_WCDCM901TH ( 'LA', NULL, L_PGM_ID, L_PARAM_INF, L_STA_DH, L_PRCD_STP_INF, 'Finished', L_XTRC_CN, L_LOAD_CN, L_DEL_CN, L_PARAM_INF, P_HIRK_SEQ_INF, P_POSI_SEQ_INF );
`
`    DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 작업종료 = [' || TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS')  || ']');
`    DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : 기준일자 = [' || P_STD_DT || ']');
`
`EXCEPTION
`    /*--------------------------------------------------------------------------------------------*/
`    /*   SP 표준  EXCEPTION CHECK !!!                                                             */
`    /*--------------------------------------------------------------------------------------------*/
`    WHEN OTHERS THEN
`       L_SQLERRM := SQLERRM;
`       DBMS_OUTPUT.PUT_LINE('L_SQLERRM='||L_SQLERRM);
`       L_RETURN := SF_WCDCM901TH (L_LBLA, NULL ,L_PGM_ID ,L_PARAM_INF,L_STA_DH ,L_PRCD_STP_INF,'Aborted', L_XTRC_CN, L_LOAD_CN ,L_DEL_CN ,L_PARAM_INF ,P_HIRK_SEQ_INF ,P_POSI_SEQ_INF ,L_SQLERRM) ;
`       DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : Exception = [기타], ERR_CODE=[' || TO_CHAR(SQLCODE) || ']');
`       DBMS_OUTPUT.PUT_LINE(L_PGM_ID || ' : Exception = [기타], ERR_MSG= [' || L_SQLERRM        || ']');
`       ROLLBACK;
`       RAISE_APPLICATION_ERROR(-20100, L_SQLERRM);
`END;"
    
             #$loadfile += "==========" + $ExcelTable.PadRight(90).Replace(" ","=") + "`n" + $loadquery + "`n===================================================================================================="
             #$selfile += $ExcelTable.PadLeft(50).PadRight(100).Replace(" ","=")
             #$loadquery > C:\Users\woori\Documents\99.source\LOADSOURCE\LCD_$table"_TG".sql
             $program > C:\Users\woori\Documents\99.source\추출PROGRAM\DW추출\SP_$ExcelTarTable.sql
         }
     }
     #$selfile > C:\Users\woori\Documents\99.source\selectALL.sql
     #$crefile > C:\Users\woori\Documents\99.source\createALL.sql
     #$loadfile > C:\Users\woori\Documents\99.source\loadALL.sql
     $ExcelWorkBook.Close()
     $con.close()
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