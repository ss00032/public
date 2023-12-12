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


try {
     $ExcelObj = new-Object -Comobject Excel.Application
     $ExcelObj.visible=$false

     $ExcelWorkBook = $ExcelObj.Workbooks.Open("D:\00.우리카드 개인화마케팅 통합플랫폼 구축\00. 업무\999. ETL영역 관련\ETL영역_관련문서.xlsx")
     $ExcelWorkSheet = $ExcelWorkBook.Sheets.Item("파워쉘작업_마케팅추출")
     
     $rowcount=$ExcelWorkSheet.UsedRange.Rows.Count

     $selfile=@()
     $crefile=@()
     $loadfile=@()

     for ($i=2; $i -le $rowcount; $i++) {
         $ExcelOwner = $ExcelWorkSheet.cells.Item($i, 5).value2
         $ExcelTable = $ExcelWorkSheet.cells.Item($i, 6).value2
         $ExcelTarTable = $ExcelWorkSheet.cells.Item($i, 8).value2
         $ExcelChgTable = $ExcelWorkSheet.cells.Item($i, 9).value2
         $ExcelParTable = $ExcelWorkSheet.cells.Item($i, 10).value2
         $ExcelParCnt = $ExcelWorkSheet.cells.Item($i, 11).value2
         $ExcelasisTable = $ExcelTable
         if ($ExcelTarTable -eq 'WCDIFS52TH') {
            $ExcelTable = $ExcelTable + '_1001'
         } elseif ($ExcelTarTable -eq 'WCDIFS17TH' -or $ExcelTarTable -eq 'WCDIFS18TH') {
            $ExcelTable = $ExcelTable + '_1'
         }

         Write-Host "--------------------" $ExcelOwner"."$ExcelTable
         #$statement = "select distinct owner, table_name, column_name, column_id from all_tab_columns where table_name = '" + $ExcelData + "' order by column_id"
         $statement = "SELECT
                      `       A.OWNER
                      `     , A.TABLE_NAME
                      `     , nvl(B.COMMENTS, '없음')       AS TABLE_COMMENTS
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
                      `                                               AND COLUMN_NAME = '''||A.COLUMN_NAME||''' ').EXTRACT('//text()'),'&apos;','''')) AS DATA_DEFAULT
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
                      `   AND (D.INDEX_NAME LIKE '%_UX' OR D.INDEX_NAME LIKE '%_PK')
                      ` WHERE A.OWNER = '" + $ExcelOwner + "'
                      `   AND A.TABLE_NAME = '" + $ExcelTable + "'
                      `   AND A.COLUMN_NAME NOT LIKE '%PSWD'
                      `   AND A.HIDDEN_COLUMN = 'NO'
                      ` ORDER BY A.OWNER
                      `        , A.TABLE_NAME
                      `        , A.COLUMN_ID"
         $con = New-Object System.Data.OracleClient.OracleConnection($connection_string)
         $con.Open()

         $cmd = $con.CreateCommand()
         $cmd.CommandText = $statement

         $result = $cmd.ExecuteReader()
         $colArr=@()
         $colArrCor=@()
         $colDataArr=@()
         $colloadArr=@()
         $cursorLogic=@()
         $cursor52Logic=@()
         $cursorChgLogic=@()

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

          #컬럼 길이 별 SELECT 쿼리 로직 (초기적재 대상)
          if ($column -eq 'SMS_SEND_TELNO' -or $column -eq 'WFG_CD') {
             continue
          } elseif ($column -eq 'SCRT_NM') {
            $colArr += "        " + (", REPLACE(REPLACE(A." + $column + ",chr(13), ''),chr(10), '') AS " + $column).PadRight(91) + "/* " + $columnnm + " */`n"
          } elseif ($datatype -eq 'VARCHAR2' -and $datalength -gt 99 -and $datalength -lt 999) {
            $colArr += "        " + (", REPLACE(REPLACE(A." + $column + ",chr(13), ''),chr(10), '') AS " + $column).PadRight(91) + "/* " + $columnnm + " */`n"
          } elseif ($datatype -eq 'VARCHAR2' -and $datalength -gt 999) {
            $colArr += "        " + (", REGEXP_REPLACE(A." + $column + ", '[^[:alpha:][:digit:][:punct:]]', ' ') AS " + $column).PadRight(91) + "/* " + $columnnm + " */`n"
          } elseif ($ExcelTarTable -eq 'WCDIFS54TC' -and $col_id -eq 2) {
            $colArr += "           A." + $column.PadRight(87) + "/* " + $columnnm + " */`n"
          } elseif ($ExcelParCnt -ne 0 -and $ExcelParTable -eq 'Y' -and $col_id -eq 1) {
            $colArr += "              A." + $column.PadRight(87) + "/* " + $columnnm + " */`n"
          } elseif ($ExcelParCnt -ne 0 -and $ExcelParTable -eq 'Y' -and $col_id -ne 1) {
            $colArr += "            , A." + $column.PadRight(87) + "/* " + $columnnm + " */`n"
          } elseif($col_id -eq 1) {
            $colArr += "           A." + $column.PadRight(87) + "/* " + $columnnm + " */`n"
          } else {
            $colArr += "        , A." + $column.PadRight(87) + "/* " + $columnnm + " */`n"
          }

          #컬럼 길이 별 SELECT 쿼리 로직 (변경적재 대상)
          if ($ExcelTarTable -eq 'WCDIFS52TH' -and ($column -eq 'CUST_ID_INFO1' -or $column -eq 'CUST_ID_INFO2' -or $column -eq 'CUST_ID_INFO3' -or $column -eq 'INFO_FLD1' -or $column -eq 'INFO_FLD2' -or $column -eq 'INFO_FLD3' -or $column -eq 'INFO_FLD4' -or $column -eq 'INFO_FLD5')) {
             continue
          } elseif ($column -eq 'SMS_SEND_TELNO' -or $column -eq 'WFG_CD' -or $column -eq 'RAND_VAL') {
             continue
          } elseif ($datatype -eq 'VARCHAR2' -and $datalength -gt 99 -and $datalength -lt 1000) {
            $colArrCor += "            " + (", REPLACE(REPLACE(A." + $column + ",chr(13), ''),chr(10), '') AS " + $column).PadRight(87) + "/* " + $columnnm + " */`n"
          } elseif ($datatype -eq 'VARCHAR2' -and $datalength -gt 999) {
            $colArrCor += "        " + (", REGEXP_REPLACE(A." + $column + ", '[^[:alpha:][:digit:][:punct:]]', ' ') AS " + $column).PadRight(91) + "/* " + $columnnm + " */`n"
          } elseif ($ExcelParCnt -ne 0 -and $ExcelParTable -eq 'Y' -and $col_id -eq 1) {
            $colArrCor += "              A." + $column.PadRight(83) + "/* " + $columnnm + " */`n"
          } elseif ($ExcelParCnt -ne 0 -and $ExcelParTable -eq 'Y' -and $col_id -ne 1) {
            $colArrCor += "            , A." + $column.PadRight(83) + "/* " + $columnnm + " */`n"
          } elseif($col_id -eq 1) {
            $colArrCor += "           A." + $column.PadRight(83) + "/* " + $columnnm + " */`n"
          } else {
            $colArrCor += "        , A." + $column.PadRight(83) + "/* " + $columnnm + " */`n"
          }

          #CURSOR 로직 생성
          if ($column -eq 'SMS_SEND_TELNO' -or $column -eq 'WFG_CD') {
             continue
          } elseif ($ExcelTarTable -eq 'WCDIFS54TC' -and $col_id -eq 2) {
            $colDataArr += $ExcelTarTable + "_REC." + $column.PadRight(65) + "/* " + $columnnm + " */`n"
          } elseif($col_id -eq 1) {
            $colDataArr += $ExcelTarTable + "_REC." + $column.PadRight(65) + "/* " + $columnnm + " */`n"
          }else {
            $colDataArr += "       ||'|'||     " + $ExcelTarTable + "_REC." + $column.PadRight(65) + "/* " + $columnnm + " */`n"
          }         
         }

         #요일별 테이블 CURSOR 로직 셋팅
         if ($ExcelParCnt -ne 0 -and $ExcelParTable -eq 'Y') {
            for ($j=1; $j -le $ExcelParCnt; $j++) {
               #52번 (총 35개 테이블) 초기,변경 cursor 로직 셋팅
               if ($ExcelTarTable -eq 'WCDIFS52TH') {

                   $cursor52Logic += "       SELECT /*+ PARALLEL(A 16) FULL(A) */`n"
                   $cursor52Logic += $colArrCor
                   $cursor52Logic += "         FROM " + $ExcelOwner + "." + ($ExcelasisTable + "_" + $j + "001 A").PadRight(78) + "/* " + $tablenm + " */`n"
                   $cursor52Logic += "        UNION ALL`n"
                   $cursor52Logic += "       SELECT /*+ PARALLEL(A 16) FULL(A) */`n"
                   $cursor52Logic += $colArrCor
                   $cursor52Logic += "         FROM " + $ExcelOwner + "." + ($ExcelasisTable + "_" + $j + "002 A").PadRight(78) + "/* " + $tablenm + " */`n"
                   $cursor52Logic += "        UNION ALL`n"
                   $cursor52Logic += "       SELECT /*+ PARALLEL(A 16) FULL(A) */`n"
                   $cursor52Logic += $colArrCor
                   $cursor52Logic += "         FROM " + $ExcelOwner + "." + ($ExcelasisTable + "_" + $j + "003 A").PadRight(78) + "/* " + $tablenm + " */`n"
                   $cursor52Logic += "        UNION ALL`n"
                   $cursor52Logic += "       SELECT /*+ PARALLEL(A 16) FULL(A) */`n"
                   $cursor52Logic += $colArrCor
                   $cursor52Logic += "         FROM " + $ExcelOwner + "." + ($ExcelasisTable + "_" + $j + "004 A").PadRight(78) + "/* " + $tablenm + " */`n"
                   $cursor52Logic += "        UNION ALL`n"
                   $cursor52Logic += "       SELECT /*+ PARALLEL(A 16) FULL(A) */`n"
                   $cursor52Logic += $colArrCor
                   $cursor52Logic += "         FROM " + $ExcelOwner + "." + ($ExcelasisTable + "_" + $j + "005 A").PadRight(78) + "/* " + $tablenm + " */`n"
                   if ($j -eq $ExcelParCnt) {
                     $cursor52Logic += "       );`n"
                   } else {
                     $cursor52Logic += "        UNION ALL`n"
                   }

                   $cursorChgLogic += "       CURSOR " + $ExcelTarTable + $j + "_CUR IS`n"
                   $cursorChgLogic += "       SELECT DISTINCT * FROM (`n"
                   $cursorChgLogic += "       SELECT /*+ PARALLEL(A 16) FULL(A) */`n"
                   $cursorChgLogic += $colArrCor
                   $cursorChgLogic += "         FROM " + $ExcelOwner + "." + ($ExcelasisTable + "_" + $j + "001 A").PadRight(78) + "/* " + $tablenm + " */`n"
                   $cursorChgLogic += "        UNION ALL`n"
                   $cursorChgLogic += "       SELECT /*+ PARALLEL(A 16) FULL(A) */`n"
                   $cursorChgLogic += $colArrCor
                   $cursorChgLogic += "         FROM " + $ExcelOwner + "." + ($ExcelasisTable + "_" + $j + "002 A").PadRight(78) + "/* " + $tablenm + " */`n"
                   $cursorChgLogic += "        UNION ALL`n"
                   $cursorChgLogic += "       SELECT /*+ PARALLEL(A 16) FULL(A) */`n"
                   $cursorChgLogic += $colArrCor
                   $cursorChgLogic += "         FROM " + $ExcelOwner + "." + ($ExcelasisTable + "_" + $j + "003 A").PadRight(78) + "/* " + $tablenm + " */`n"
                   $cursorChgLogic += "        UNION ALL`n"
                   $cursorChgLogic += "       SELECT /*+ PARALLEL(A 16) FULL(A) */`n"
                   $cursorChgLogic += $colArrCor
                   $cursorChgLogic += "         FROM " + $ExcelOwner + "." + ($ExcelasisTable + "_" + $j + "004 A").PadRight(78) + "/* " + $tablenm + " */`n"
                   $cursorChgLogic += "        UNION ALL`n"
                   $cursorChgLogic += "       SELECT /*+ PARALLEL(A 16) FULL(A) */`n"
                   $cursorChgLogic += $colArrCor
                   $cursorChgLogic += "         FROM " + $ExcelOwner + "." + ($ExcelasisTable + "_" + $j + "005 A").PadRight(78) + "/* " + $tablenm + " */`n"
                   $cursorChgLogic += "   );`n"
               #17,18번 (7개 테이블) cursor 로직 셋팅
               } else {
                   $cursorLogic += "       SELECT /*+ PARALLEL(A 16) FULL(A) */`n"
                   $cursorLogic += $colArrCor
                   $cursorLogic += "         FROM " + $ExcelOwner + "." + ($ExcelasisTable + "_" + $j + " A").PadRight(78) + "/* " + $tablenm + " */`n"
                   if ($j -eq $ExcelParCnt) {
                     $cursorLogic += "   ;`n"
                   } else {
                     $cursorLogic += "        UNION ALL`n"
                   }

                   $cursorChgLogic += "       CURSOR " + $ExcelTarTable + $j + "_CUR IS`n"
                   $cursorChgLogic += "       SELECT /*+ PARALLEL(A 16) FULL(A) */`n"
                   $cursorChgLogic += $colArrCor
                   $cursorChgLogic += "         FROM " + $ExcelOwner + "." + ($ExcelasisTable + "_" + $j + " A").PadRight(78) + "/* " + $tablenm + " */`n"
                   $cursorChgLogic += "   ;`n"
               }
             }
         }
         #변경적재 대상 X , 테이블 분할 대상 X
         if($ExcelChgTable -eq 'X' -and $ExcelParTable -eq 'X') {

            $programini = "DECLARE`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* 개별변수 선언                                                                              */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    V_CNT       NUMBER(15);                                                                         /* 건수 */`n"
            $programini += "    DIRLOC              VARCHAR2(300)  := NULL;                                                     /* DIR경로 */`n"
            $programini += "    SAMFILE_W           UTL_FILE.FILE_TYPE;                                                         /* SAM WRITE */`n"
            $programini += "    SAM_FILE            VARCHAR2(5000) := NULL;                                                     /* SAM_FILE */`n"
            $programini += "    L_FILE_ID           VARCHAR2(30);   -- FILE ID                                                  /* FILE_ID */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* 추출 DATA  CURSOR                                                                          */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    CURSOR " + $ExcelTarTable + "_CUR IS`n"
            $programini += "    SELECT /*+ PARALLEL(A 16) FULL(A) */`n"
            $programini += $colArr
            $programini += "      FROM " + $ExcelOwner + "." + ($ExcelTable + " A").PadRight(82) + "/* " + $tablenm + " */`n"
            $programini += "    ;`n"
            $programini += "`n"
            $programini += "BEGIN`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* 공통/개별 변수값 Setting                                                                   */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* OWNER, 타겟테이블ID, TEMP테이블ID, PARTITION_ID, 배치프로그램명 등은 PMPHead.sql에서 셋팅  */`n"
            $programini += "    /* 개발 시 추가로 필요한 공통/개별 변수값 Setting 영역                                        */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    V_CNT := 0;                                                                                     /* 건수 개별변수 Setting */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* SAMFILE WRITE                                                                              */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* FILE_ID : TABLE명_YYYYMMDD.dat                                                             */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    P_BAT_STG_N  := LPAD(TO_NUMBER(P_BAT_STG_N)+1,3,'0');                                           /* STEP번호 */`n"
            $programini += "    P_BAT_STG_NM := P_TAR_TBL_ID1 || ' TABLE TRUNCATE';                                             /* STEP명 */`n"
            $programini += "    P_RM_SR_DT   := TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISSFF2');                                   /* STEP시작시간 */`n"
            $programini += "`n"
            $programini += "    DIRLOC := P_SEND_DIR_LOC;                                                                       /* 마케팅DB SAM파일 저장경로 */`n"
            $programini += "    L_FILE_ID := SUBSTR(P_PMP_BAT_PGM_ID,5,10)||'_'||P_CED||'.dat';                                 /* FILE ID */`n"
            $programini += "    SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',6400);                                       /* DAT 파일(BUFFER MAX(6400)) */`n"
            $programini += "    P_PS_CT := 0;`n"
            $programini += "`n"
            $programini += "    FOR " + $ExcelTarTable + "_REC IN " + $ExcelTarTable + "_CUR LOOP"
            $programini += "`n"
            $programini += "        SAM_FILE := "
            $programini += $colDataArr
            $programini += "        ||'|'`n"
            $programini += "        ;`n"
            $programini += "        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);"
            $programini += "`n"
            $programini += "        P_PS_CT := P_PS_CT + 1;"
            $programini += "`n"
            $programini += "    END LOOP;`n"
            $programini += "`n"         
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /*   Sam File close                                                                           */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    UTL_FILE.FCLOSE(SAMFILE_W);`n"
            $programini += "`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* 최종 로그                                                                                  */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    P_BTJ_ED_F  := 'Y';                                                                             /* 최종작업여부(최종작업인 경우 반드시 기술(예시)) */`n"
            $programini += "    SP_LOG_MRT(P_PMP_BAT_PGM_ID,P_PMP_BAT_PGM_NM,P_CED,P_BAT_STG_N,P_BAT_STG_NM,P_RM_NI_SR_DT,P_RM_SR_DT,P_PS_CT,P_BAT_ERO_CD,P_BAT_ERO_MSG_TT,P_SSU_CD,P_BTJ_ED_F,P_RTN);`n"
            $programini += "`n"
            $programini += "    DBMS_APPLICATION_INFO.SET_ACTION(P_PMP_BAT_PGM_ID||' END');`n"
            $programini += "`n"
            $programini += "EXCEPTION`n"
            $programini += "`n"
            $programini += "    WHEN USER_EXCEPTION THEN`n"
            $programini += "    GOTO ERR_RETURN;`n"
            $programini += "`n"
            $programini += "    WHEN OTHERS THEN`n"
            $programini += "    GOTO ERR_RETURN;`n"
            $programini += "`n"
            $programini += "    <<ERR_RETURN>>`n"
            $programini += "`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /*                               SET 적재 LOG 변수(오류)                                      */`n"
            $programini += "    /*                         ERROR발생시 LOG테이블에 ERROR코드 입력                             */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    IF P_SSU_CD = 'ERR'THEN`n"
            $programini += "       P_BAT_ERO_CD     := SQLCODE;`n"
            $programini += "    ELSE`n"
            $programini += "       P_BAT_ERO_CD     := SQLCODE;`n"
            $programini += "       P_BAT_ERO_MSG_TT := SQLERRM;`n"
            $programini += "    END IF;`n"
            $programini += "`n"
            $programini += "END;`n"

            #$programini | Set-Content -Encoding utf8 "C:\Users\woori\Documents\99.source\추출PROGRAM\마케팅DB추출\초기\UCD_" + $ExcelTarTable + "_TG".sql
            $programini > C:\Users\woori\Documents\99.source\추출PROGRAM\마케팅DB추출\초기\UCD_$ExcelTarTable"_TG".sql
        #변경적재 대상 X , 테이블 분할 대상 Y
        } elseif($ExcelChgTable -eq 'X' -and $ExcelParTable -eq 'Y') {
            $programini = "DECLARE`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* 개별변수 선언                                                                              */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    V_CNT       NUMBER(15);                                                                         /* 건수 */`n"
            $programini += "    DIRLOC              VARCHAR2(300)  := NULL;                                                     /* DIR경로 */`n"
            $programini += "    SAMFILE_W           UTL_FILE.FILE_TYPE;                                                         /* SAM WRITE */`n"
            $programini += "    SAM_FILE            VARCHAR2(5000) := NULL;                                                     /* SAM_FILE */`n"
            $programini += "    L_FILE_ID           VARCHAR2(30);   -- FILE ID                                                  /* FILE_ID */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* 추출 DATA  CURSOR                                                                          */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += $cursorLogic
            $programini += "`n"
            $programini += "BEGIN`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* 공통/개별 변수값 Setting                                                                   */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* OWNER, 타겟테이블ID, TEMP테이블ID, PARTITION_ID, 배치프로그램명 등은 PMPHead.sql에서 셋팅  */`n"
            $programini += "    /* 개발 시 추가로 필요한 공통/개별 변수값 Setting 영역                                        */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    V_CNT := 0;                                                                                     /* 건수 개별변수 Setting */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* SAMFILE WRITE                                                                              */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* FILE_ID : TABLE명_YYYYMMDD.dat                                                             */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    P_BAT_STG_N  := LPAD(TO_NUMBER(P_BAT_STG_N)+1,3,'0');                                           /* STEP번호 */`n"
            $programini += "    P_BAT_STG_NM := P_TAR_TBL_ID1 || ' TABLE TRUNCATE';                                             /* STEP명 */`n"
            $programini += "    P_RM_SR_DT   := TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISSFF2');                                   /* STEP시작시간 */`n"
            $programini += "`n"
            $programini += "    DIRLOC := P_SEND_DIR_LOC;                                                                       /* 마케팅DB SAM파일 저장경로 */`n"
            $programini += "    L_FILE_ID := SUBSTR(P_PMP_BAT_PGM_ID,5,10)||'_'||P_CED||'.dat';                                 /* FILE ID */`n"
            $programini += "    SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',6400);                                       /* DAT 파일(BUFFER MAX(6400)) */`n"
            $programini += "    P_PS_CT := 0;`n"
            $programini += "`n"
            $programini += "    FOR " + $ExcelTarTable + "_REC IN " + $ExcelTarTable + "_CUR LOOP"
            $programini += "`n"
            $programini += "        SAM_FILE := "
            $programini += $colDataArr
            $programini += "        ||'|'`n"
            $programini += "        ;`n"
            $programini += "        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);"
            $programini += "`n"
            $programini += "        P_PS_CT := P_PS_CT + 1;"
            $programini += "`n"
            $programini += "    END LOOP;`n"
            $programini += "`n"         
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /*   Sam File close                                                                           */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    UTL_FILE.FCLOSE(SAMFILE_W);`n"
            $programini += "`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* 최종 로그                                                                                  */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    P_BTJ_ED_F  := 'Y';                                                                             /* 최종작업여부(최종작업인 경우 반드시 기술(예시)) */`n"
            $programini += "    SP_LOG_MRT(P_PMP_BAT_PGM_ID,P_PMP_BAT_PGM_NM,P_CED,P_BAT_STG_N,P_BAT_STG_NM,P_RM_NI_SR_DT,P_RM_SR_DT,P_PS_CT,P_BAT_ERO_CD,P_BAT_ERO_MSG_TT,P_SSU_CD,P_BTJ_ED_F,P_RTN);`n"
            $programini += "`n"
            $programini += "    DBMS_APPLICATION_INFO.SET_ACTION(P_PMP_BAT_PGM_ID||' END');`n"
            $programini += "`n"
            $programini += "EXCEPTION`n"
            $programini += "`n"
            $programini += "    WHEN USER_EXCEPTION THEN`n"
            $programini += "    GOTO ERR_RETURN;`n"
            $programini += "`n"
            $programini += "    WHEN OTHERS THEN`n"
            $programini += "    GOTO ERR_RETURN;`n"
            $programini += "`n"
            $programini += "    <<ERR_RETURN>>`n"
            $programini += "`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /*                               SET 적재 LOG 변수(오류)                                      */`n"
            $programini += "    /*                         ERROR발생시 LOG테이블에 ERROR코드 입력                             */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    IF P_SSU_CD = 'ERR'THEN`n"
            $programini += "       P_BAT_ERO_CD     := SQLCODE;`n"
            $programini += "    ELSE`n"
            $programini += "       P_BAT_ERO_CD     := SQLCODE;`n"
            $programini += "       P_BAT_ERO_MSG_TT := SQLERRM;`n"
            $programini += "    END IF;`n"
            $programini += "`n"
            $programini += "END;`n"

            #$programini | Set-Content -Encoding utf8 "C:\Users\woori\Documents\99.source\추출PROGRAM\마케팅DB추출\초기\UCD_" + $ExcelTarTable + "_TG".sql
            $programini > C:\Users\woori\Documents\99.source\추출PROGRAM\마케팅DB추출\초기\UCD_$ExcelTarTable"_TG".sql
        #변경적재 대상 , 테이블 분할 대상 Y, 52번 테이블
        } elseif($ExcelChgTable -ne 'X' -and $ExcelParTable -eq 'Y' -and $ExcelTarTable -eq 'WCDIFS52TH') {
            $programini = "DECLARE`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* 개별변수 선언                                                                              */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    V_CNT       NUMBER(15);                                                                         /* 건수 */`n"
            $programini += "    DIRLOC              VARCHAR2(300)  := NULL;                                                     /* DIR경로 */`n"
            $programini += "    SAMFILE_W           UTL_FILE.FILE_TYPE;                                                         /* SAM WRITE */`n"
            $programini += "    SAM_FILE            VARCHAR2(5000) := NULL;                                                     /* SAM_FILE */`n"
            $programini += "    L_FILE_ID           VARCHAR2(30);   -- FILE ID                                                  /* FILE_ID */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* 추출 DATA  CURSOR                                                                          */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    CURSOR " + $ExcelTarTable + "_CUR IS`n"
            $programini += "    SELECT DISTINCT * FROM (`n"
            $programini += $cursor52Logic
            $programini += "`n"
            $programini += "BEGIN`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* 공통/개별 변수값 Setting                                                                   */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* OWNER, 타겟테이블ID, TEMP테이블ID, PARTITION_ID, 배치프로그램명 등은 PMPHead.sql에서 셋팅  */`n"
            $programini += "    /* 개발 시 추가로 필요한 공통/개별 변수값 Setting 영역                                        */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    V_CNT := 0;                                                                                     /* 건수 개별변수 Setting */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* SAMFILE WRITE                                                                              */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* FILE_ID : TABLE명_YYYYMMDD.dat                                                             */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    P_BAT_STG_N  := LPAD(TO_NUMBER(P_BAT_STG_N)+1,3,'0');                                           /* STEP번호 */`n"
            $programini += "    P_BAT_STG_NM := P_TAR_TBL_ID1 || ' TABLE TRUNCATE';                                             /* STEP명 */`n"
            $programini += "    P_RM_SR_DT   := TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISSFF2');                                   /* STEP시작시간 */`n"
            $programini += "`n"
            $programini += "    DIRLOC := P_SEND_DIR_LOC;`n"
            $programini += "    L_FILE_ID := SUBSTR(P_PMP_BAT_PGM_ID,5,10)||'_'||P_CED||'.dat';                                 /* FILE ID */`n"
            $programini += "    SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',6400);                                       /* DAT 파일(BUFFER MAX(6400)) */`n"
            $programini += "    P_PS_CT := 0;`n"
            $programini += "`n"
            $programini += "    FOR " + $ExcelTarTable + "_REC IN " + $ExcelTarTable + "_CUR LOOP"
            $programini += "`n"
            $programini += "        SAM_FILE := "
            $programini += $colDataArr
            $programini += "        ||'|'`n"
            $programini += "        ;`n"
            $programini += "        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);"
            $programini += "`n"
            $programini += "        P_PS_CT := P_PS_CT + 1;"
            $programini += "`n"
            $programini += "    END LOOP;`n"
            $programini += "`n"         
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /*   Sam File close                                                                           */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    UTL_FILE.FCLOSE(SAMFILE_W);`n"
            $programini += "`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* 최종 로그                                                                                  */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    P_BTJ_ED_F  := 'Y';                                                                             /* 최종작업여부(최종작업인 경우 반드시 기술(예시)) */`n"
            $programini += "    SP_LOG_MRT(P_PMP_BAT_PGM_ID,P_PMP_BAT_PGM_NM,P_CED,P_BAT_STG_N,P_BAT_STG_NM,P_RM_NI_SR_DT,P_RM_SR_DT,P_PS_CT,P_BAT_ERO_CD,P_BAT_ERO_MSG_TT,P_SSU_CD,P_BTJ_ED_F,P_RTN);`n"
            $programini += "`n"
            $programini += "    DBMS_APPLICATION_INFO.SET_ACTION(P_PMP_BAT_PGM_ID||' END');`n"
            $programini += "`n"
            $programini += "EXCEPTION`n"
            $programini += "`n"
            $programini += "    WHEN USER_EXCEPTION THEN`n"
            $programini += "    GOTO ERR_RETURN;`n"
            $programini += "`n"
            $programini += "    WHEN OTHERS THEN`n"
            $programini += "    GOTO ERR_RETURN;`n"
            $programini += "`n"
            $programini += "    <<ERR_RETURN>>`n"
            $programini += "`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /*                               SET 적재 LOG 변수(오류)                                      */`n"
            $programini += "    /*                         ERROR발생시 LOG테이블에 ERROR코드 입력                             */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    IF P_SSU_CD = 'ERR'THEN`n"
            $programini += "       P_BAT_ERO_CD     := SQLCODE;`n"
            $programini += "    ELSE`n"
            $programini += "       P_BAT_ERO_CD     := SQLCODE;`n"
            $programini += "       P_BAT_ERO_MSG_TT := SQLERRM;`n"
            $programini += "    END IF;`n"
            $programini += "`n"
            $programini += "END;`n"

            #$programini | Set-Content -Encoding utf8 "C:\Users\woori\Documents\99.source\추출PROGRAM\마케팅DB추출\초기\UID_" + $ExcelTarTable + "_TG".sql
            $programini > C:\Users\woori\Documents\99.source\추출PROGRAM\마케팅DB추출\초기\UID_$ExcelTarTable"_TG".sql

            $programchg = "DECLARE`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* 개별변수 선언                                                                              */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    V_CNT       NUMBER(15);                                                                         /* 건수 */`n"
            $programchg += "    DIRLOC              VARCHAR2(300)  := NULL;                                                     /* DIR경로 */`n"
            $programchg += "    SAMFILE_W           UTL_FILE.FILE_TYPE;                                                         /* SAM WRITE */`n"
            $programchg += "    SAM_FILE            VARCHAR2(5000) := NULL;                                                     /* SAM_FILE */`n"
            $programchg += "    L_FILE_ID           VARCHAR2(30);   -- FILE ID                                                  /* FILE_ID */`n"
            $programchg += "    L_DAY_CHECK         VARCHAR2(1);                                                                /* 요일체크 */`n"                  
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* 추출 DATA  CURSOR                                                                          */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += " " + $cursorChgLogic
            $programchg += "`n"
            $programchg += "BEGIN`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* 공통/개별 변수값 Setting                                                                   */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* OWNER, 타겟테이블ID, TEMP테이블ID, PARTITION_ID, 배치프로그램명 등은 PMPHead.sql에서 셋팅  */`n"
            $programchg += "    /* 개발 시 추가로 필요한 공통/개별 변수값 Setting 영역                                        */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    V_CNT := 0;                                                                                     /* 건수 개별변수 Setting */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* SAMFILE WRITE                                                                              */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* FILE_ID : TABLE명_YYYYMMDD.dat                                                             */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    P_BAT_STG_N  := LPAD(TO_NUMBER(P_BAT_STG_N)+1,3,'0');                                           /* STEP번호 */`n"
            $programchg += "    P_BAT_STG_NM := P_TAR_TBL_ID1 || ' TABLE TRUNCATE';                                             /* STEP명 */`n"
            $programchg += "    P_RM_SR_DT   := TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISSFF2');                                   /* STEP시작시간 */`n"
            $programchg += "`n"    
            $programchg += "`n"
            $programchg += "    SELECT TO_CHAR(TO_DATE(P_CED,'YYYYMMDD'), 'd') INTO L_DAY_CHECK FROM DUAL;`n"
            $programchg += "`n"
            $programchg += "    IF L_DAY_CHECK = 1 THEN`n"
            $programchg += "       DIRLOC := P_SEND_DIR_LOC;`n"
            $programchg += "       L_FILE_ID := SUBSTR(P_PMP_BAT_PGM_ID,5,10)||'_'||P_CED||'.dat';                              /* FILE ID */`n"
            $programchg += "       SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',6400);                                    /* DAT 파일(BUFFER MAX(6400)) */`n"
            $programchg += "       P_PS_CT := 0;`n"
            $programchg += "`n"
            $programchg += "       FOR " + $ExcelTarTable + "_REC IN " + $ExcelTarTable + "1_CUR LOOP"
            $programchg += "`n"
            $programchg += "        SAM_FILE := "
            $programchg += $colDataArr
            $programchg += "        ||'|'`n"
            $programchg += "        ;`n"
            $programchg += "        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);"
            $programchg += "`n"
            $programchg += "        P_PS_CT := P_PS_CT + 1;"
            $programchg += "`n"
            $programchg += "       END LOOP;`n"
            $programchg += "    ELSIF L_DAY_CHECK = 2 THEN`n"
            $programchg += "       DIRLOC := P_SEND_DIR_LOC;`n"
            $programchg += "       L_FILE_ID := SUBSTR(P_PMP_BAT_PGM_ID,5,10)||'_'||P_CED||'.dat';                              /* FILE ID */`n"
            $programchg += "       SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',6400);                                    /* DAT 파일(BUFFER MAX(6400)) */`n"
            $programchg += "       P_PS_CT := 0;`n"
            $programchg += "`n"
            $programchg += "       FOR " + $ExcelTarTable + "_REC IN " + $ExcelTarTable + "2_CUR LOOP"
            $programchg += "`n"
            $programchg += "        SAM_FILE := "
            $programchg += $colDataArr
            $programchg += "        ||'|'`n"
            $programchg += "        ;`n"
            $programchg += "        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);"
            $programchg += "`n"
            $programchg += "        P_PS_CT := P_PS_CT + 1;"
            $programchg += "`n"
            $programchg += "       END LOOP;`n"
            $programchg += "    ELSIF L_DAY_CHECK = 3 THEN`n"
            $programchg += "       DIRLOC := P_SEND_DIR_LOC;`n"
            $programchg += "       L_FILE_ID := SUBSTR(P_PMP_BAT_PGM_ID,5,10)||'_'||P_CED||'.dat';                              /* FILE ID */`n"
            $programchg += "       SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',6400);                                    /* DAT 파일(BUFFER MAX(6400)) */`n"
            $programchg += "       P_PS_CT := 0;`n"
            $programchg += "`n"
            $programchg += "       FOR " + $ExcelTarTable + "_REC IN " + $ExcelTarTable + "3_CUR LOOP"
            $programchg += "`n"
            $programchg += "        SAM_FILE := "
            $programchg += $colDataArr
            $programchg += "        ||'|'`n"
            $programchg += "        ;`n"
            $programchg += "        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);"
            $programchg += "`n"
            $programchg += "        P_PS_CT := P_PS_CT + 1;"
            $programchg += "`n"
            $programchg += "       END LOOP;`n"
            $programchg += "    ELSIF L_DAY_CHECK = 4 THEN`n"
            $programchg += "       DIRLOC := P_SEND_DIR_LOC;`n"
            $programchg += "       L_FILE_ID := SUBSTR(P_PMP_BAT_PGM_ID,5,10)||'_'||P_CED||'.dat';                              /* FILE ID */`n"
            $programchg += "       SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',6400);                                    /* DAT 파일(BUFFER MAX(6400)) */`n"
            $programchg += "       P_PS_CT := 0;`n"
            $programchg += "`n"
            $programchg += "       FOR " + $ExcelTarTable + "_REC IN " + $ExcelTarTable + "4_CUR LOOP"
            $programchg += "`n"
            $programchg += "        SAM_FILE := "
            $programchg += $colDataArr
            $programchg += "        ||'|'`n"
            $programchg += "        ;`n"
            $programchg += "        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);"
            $programchg += "`n"
            $programchg += "        P_PS_CT := P_PS_CT + 1;"
            $programchg += "`n"
            $programchg += "       END LOOP;`n"
            $programchg += "    ELSIF L_DAY_CHECK = 5 THEN`n"
            $programchg += "       DIRLOC := P_SEND_DIR_LOC;`n"
            $programchg += "       L_FILE_ID := SUBSTR(P_PMP_BAT_PGM_ID,5,10)||'_'||P_CED||'.dat';                              /* FILE ID */`n"
            $programchg += "       SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',6400);                                    /* DAT 파일(BUFFER MAX(6400)) */`n"
            $programchg += "       P_PS_CT := 0;`n"
            $programchg += "`n"
            $programchg += "       FOR " + $ExcelTarTable + "_REC IN " + $ExcelTarTable + "5_CUR LOOP"
            $programchg += "`n"
            $programchg += "        SAM_FILE := "
            $programchg += $colDataArr
            $programchg += "        ||'|'`n"
            $programchg += "        ;`n"
            $programchg += "        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);"
            $programchg += "`n"
            $programchg += "        P_PS_CT := P_PS_CT + 1;"
            $programchg += "`n"
            $programchg += "       END LOOP;`n"
            $programchg += "    ELSIF L_DAY_CHECK = 6 THEN`n"
            $programchg += "       DIRLOC := P_SEND_DIR_LOC;`n"
            $programchg += "       L_FILE_ID := SUBSTR(P_PMP_BAT_PGM_ID,5,10)||'_'||P_CED||'.dat';                              /* FILE ID */`n"
            $programchg += "       SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',6400);                                    /* DAT 파일(BUFFER MAX(6400)) */`n"
            $programchg += "       P_PS_CT := 0;`n"
            $programchg += "`n"
            $programchg += "       FOR " + $ExcelTarTable + "_REC IN " + $ExcelTarTable + "6_CUR LOOP"
            $programchg += "`n"
            $programchg += "        SAM_FILE := "
            $programchg += $colDataArr
            $programchg += "        ||'|'`n"
            $programchg += "        ;`n"
            $programchg += "        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);"
            $programchg += "`n"
            $programchg += "        P_PS_CT := P_PS_CT + 1;"
            $programchg += "`n"
            $programchg += "       END LOOP;`n"
            $programchg += "    ELSIF L_DAY_CHECK = 7 THEN`n"
            $programchg += "       DIRLOC := P_SEND_DIR_LOC;`n"
            $programchg += "       L_FILE_ID := SUBSTR(P_PMP_BAT_PGM_ID,5,10)||'_'||P_CED||'.dat';                              /* FILE ID */`n"
            $programchg += "       SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',6400);                                    /* DAT 파일(BUFFER MAX(6400)) */`n"
            $programchg += "       P_PS_CT := 0;`n"
            $programchg += "`n"
            $programchg += "       FOR " + $ExcelTarTable + "_REC IN " + $ExcelTarTable + "7_CUR LOOP"
            $programchg += "`n"
            $programchg += "        SAM_FILE := "
            $programchg += $colDataArr
            $programchg += "        ||'|'`n"
            $programchg += "        ;`n"
            $programchg += "        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);"
            $programchg += "`n"
            $programchg += "        P_PS_CT := P_PS_CT + 1;"
            $programchg += "`n"
            $programchg += "       END LOOP;`n"
            $programchg += "    END IF;`n"
            $programchg += "`n"         
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /*   Sam File close                                                                           */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    UTL_FILE.FCLOSE(SAMFILE_W);`n"
            $programchg += "`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* 최종 로그                                                                                  */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    P_BTJ_ED_F  := 'Y';                                                                             /* 최종작업여부(최종작업인 경우 반드시 기술(예시)) */`n"
            $programchg += "    SP_LOG_MRT(P_PMP_BAT_PGM_ID,P_PMP_BAT_PGM_NM,P_CED,P_BAT_STG_N,P_BAT_STG_NM,P_RM_NI_SR_DT,P_RM_SR_DT,P_PS_CT,P_BAT_ERO_CD,P_BAT_ERO_MSG_TT,P_SSU_CD,P_BTJ_ED_F,P_RTN);`n"
            $programchg += "`n"
            $programchg += "    DBMS_APPLICATION_INFO.SET_ACTION(P_PMP_BAT_PGM_ID||' END');`n"
            $programchg += "`n"
            $programchg += "EXCEPTION`n"
            $programchg += "`n"
            $programchg += "    WHEN USER_EXCEPTION THEN`n"
            $programchg += "    GOTO ERR_RETURN;`n"
            $programchg += "`n"
            $programchg += "    WHEN OTHERS THEN`n"
            $programchg += "    GOTO ERR_RETURN;`n"
            $programchg += "`n"
            $programchg += "    <<ERR_RETURN>>`n"
            $programchg += "`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /*                               SET 적재 LOG 변수(오류)                                      */`n"
            $programchg += "    /*                         ERROR발생시 LOG테이블에 ERROR코드 입력                             */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    IF P_SSU_CD = 'ERR'THEN`n"
            $programchg += "       P_BAT_ERO_CD     := SQLCODE;`n"
            $programchg += "    ELSE`n"
            $programchg += "       P_BAT_ERO_CD     := SQLCODE;`n"
            $programchg += "       P_BAT_ERO_MSG_TT := SQLERRM;`n"
            $programchg += "    END IF;`n"
            $programchg += "`n"
            $programchg += "END;`n"
            
            #$programini | Set-Content -Encoding utf8 "C:\Users\woori\Documents\99.source\추출PROGRAM\마케팅DB추출\변경\UCD_" + $ExcelTarTable + "_TG".sql
            $programchg > C:\Users\woori\Documents\99.source\추출PROGRAM\마케팅DB추출\변경\UCD_$ExcelTarTable"_TG".sql
        #변경적재 대상 , 테이블 분할 대상 Y, 17,18번
        } elseif($ExcelChgTable -ne 'X' -and $ExcelParTable -eq 'Y') {
            $programini = "DECLARE`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* 개별변수 선언                                                                              */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    V_CNT       NUMBER(15);                                                                         /* 건수 */`n"
            $programini += "    DIRLOC              VARCHAR2(300)  := NULL;                                                     /* DIR경로 */`n"
            $programini += "    SAMFILE_W           UTL_FILE.FILE_TYPE;                                                         /* SAM WRITE */`n"
            $programini += "    SAM_FILE            VARCHAR2(5000) := NULL;                                                     /* SAM_FILE */`n"
            $programini += "    L_FILE_ID           VARCHAR2(30);   -- FILE ID                                                  /* FILE_ID */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* 추출 DATA  CURSOR                                                                          */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    CURSOR " + $ExcelTarTable + "_CUR IS`n"
            $programini += $cursorLogic
            $programini += "`n"
            $programini += "BEGIN`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* 공통/개별 변수값 Setting                                                                   */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* OWNER, 타겟테이블ID, TEMP테이블ID, PARTITION_ID, 배치프로그램명 등은 PMPHead.sql에서 셋팅  */`n"
            $programini += "    /* 개발 시 추가로 필요한 공통/개별 변수값 Setting 영역                                        */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    V_CNT := 0;                                                                                     /* 건수 개별변수 Setting */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* SAMFILE WRITE                                                                              */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* FILE_ID : TABLE명_YYYYMMDD.dat                                                             */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    P_BAT_STG_N  := LPAD(TO_NUMBER(P_BAT_STG_N)+1,3,'0');                                           /* STEP번호 */`n"
            $programini += "    P_BAT_STG_NM := P_TAR_TBL_ID1 || ' TABLE TRUNCATE';                                             /* STEP명 */`n"
            $programini += "    P_RM_SR_DT   := TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISSFF2');                                   /* STEP시작시간 */`n"
            $programini += "`n"
            $programini += "    DIRLOC := P_SEND_DIR_LOC;`n"
            $programini += "    L_FILE_ID := SUBSTR(P_PMP_BAT_PGM_ID,5,10)||'_'||P_CED||'.dat';                                 /* FILE ID */`n"
            $programini += "    SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',6400);                                       /* DAT 파일(BUFFER MAX(6400)) */`n"
            $programini += "    P_PS_CT := 0;`n"
            $programini += "`n"
            $programini += "    FOR " + $ExcelTarTable + "_REC IN " + $ExcelTarTable + "_CUR LOOP"
            $programini += "`n"
            $programini += "        SAM_FILE := "
            $programini += $colDataArr
            $programini += "        ||'|'`n"
            $programini += "        ;`n"
            $programini += "        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);"
            $programini += "`n"
            $programini += "        P_PS_CT := P_PS_CT + 1;"
            $programini += "`n"
            $programini += "    END LOOP;`n"
            $programini += "`n"         
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /*   Sam File close                                                                           */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    UTL_FILE.FCLOSE(SAMFILE_W);`n"
            $programini += "`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* 최종 로그                                                                                  */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    P_BTJ_ED_F  := 'Y';                                                                             /* 최종작업여부(최종작업인 경우 반드시 기술(예시)) */`n"
            $programini += "    SP_LOG_MRT(P_PMP_BAT_PGM_ID,P_PMP_BAT_PGM_NM,P_CED,P_BAT_STG_N,P_BAT_STG_NM,P_RM_NI_SR_DT,P_RM_SR_DT,P_PS_CT,P_BAT_ERO_CD,P_BAT_ERO_MSG_TT,P_SSU_CD,P_BTJ_ED_F,P_RTN);`n"
            $programini += "`n"
            $programini += "    DBMS_APPLICATION_INFO.SET_ACTION(P_PMP_BAT_PGM_ID||' END');`n"
            $programini += "`n"
            $programini += "EXCEPTION`n"
            $programini += "`n"
            $programini += "    WHEN USER_EXCEPTION THEN`n"
            $programini += "    GOTO ERR_RETURN;`n"
            $programini += "`n"
            $programini += "    WHEN OTHERS THEN`n"
            $programini += "    GOTO ERR_RETURN;`n"
            $programini += "`n"
            $programini += "    <<ERR_RETURN>>`n"
            $programini += "`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /*                               SET 적재 LOG 변수(오류)                                      */`n"
            $programini += "    /*                         ERROR발생시 LOG테이블에 ERROR코드 입력                             */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    IF P_SSU_CD = 'ERR'THEN`n"
            $programini += "       P_BAT_ERO_CD     := SQLCODE;`n"
            $programini += "    ELSE`n"
            $programini += "       P_BAT_ERO_CD     := SQLCODE;`n"
            $programini += "       P_BAT_ERO_MSG_TT := SQLERRM;`n"
            $programini += "    END IF;`n"
            $programini += "`n"
            $programini += "END;`n"

            #$programini | Set-Content -Encoding utf8 "C:\Users\woori\Documents\99.source\추출PROGRAM\마케팅DB추출\초기\UID_" + $ExcelTarTable + "_TG".sql
            $programini > C:\Users\woori\Documents\99.source\추출PROGRAM\마케팅DB추출\초기\UID_$ExcelTarTable"_TG".sql

            $programchg = "DECLARE`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* 개별변수 선언                                                                              */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    V_CNT       NUMBER(15);                                                                         /* 건수 */`n"
            $programchg += "    DIRLOC              VARCHAR2(300)  := NULL;                                                     /* DIR경로 */`n"
            $programchg += "    SAMFILE_W           UTL_FILE.FILE_TYPE;                                                         /* SAM WRITE */`n"
            $programchg += "    SAM_FILE            VARCHAR2(5000) := NULL;                                                     /* SAM_FILE */`n"
            $programchg += "    L_FILE_ID           VARCHAR2(30);   -- FILE ID                                                  /* FILE_ID */`n"
            $programchg += "    L_DAY_CHECK         VARCHAR2(1);                                                                /* 요일체크 */`n"                  
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* 추출 DATA  CURSOR                                                                          */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += " " + $cursorChgLogic
            $programchg += "`n"
            $programchg += "BEGIN`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* 공통/개별 변수값 Setting                                                                   */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* OWNER, 타겟테이블ID, TEMP테이블ID, PARTITION_ID, 배치프로그램명 등은 PMPHead.sql에서 셋팅  */`n"
            $programchg += "    /* 개발 시 추가로 필요한 공통/개별 변수값 Setting 영역                                        */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    V_CNT := 0;                                                                                     /* 건수 개별변수 Setting */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* SAMFILE WRITE                                                                              */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* FILE_ID : TABLE명_YYYYMMDD.dat                                                             */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    P_BAT_STG_N  := LPAD(TO_NUMBER(P_BAT_STG_N)+1,3,'0');                                           /* STEP번호 */`n"
            $programchg += "    P_BAT_STG_NM := P_TAR_TBL_ID1 || ' TABLE TRUNCATE';                                             /* STEP명 */`n"
            $programchg += "    P_RM_SR_DT   := TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISSFF2');                                   /* STEP시작시간 */`n"
            $programchg += "`n"    
            $programchg += "`n"
            $programchg += "    SELECT TO_CHAR(TO_DATE(P_CED,'YYYYMMDD'), 'd') INTO L_DAY_CHECK FROM DUAL;`n"
            $programchg += "`n"
            $programchg += "    IF L_DAY_CHECK = 1 THEN`n"
            $programchg += "       DIRLOC := P_SEND_DIR_LOC;`n"
            $programchg += "       L_FILE_ID := SUBSTR(P_PMP_BAT_PGM_ID,5,10)||'_'||P_CED||'.dat';                              /* FILE ID */`n"
            $programchg += "       SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',6400);                                    /* DAT 파일(BUFFER MAX(6400)) */`n"
            $programchg += "       P_PS_CT := 0;`n"
            $programchg += "`n"
            $programchg += "       FOR " + $ExcelTarTable + "_REC IN " + $ExcelTarTable + "1_CUR LOOP"
            $programchg += "`n"
            $programchg += "        SAM_FILE := "
            $programchg += $colDataArr
            $programchg += "        ||'|'`n"
            $programchg += "        ;`n"
            $programchg += "        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);"
            $programchg += "`n"
            $programchg += "        P_PS_CT := P_PS_CT + 1;"
            $programchg += "`n"
            $programchg += "       END LOOP;`n"
            $programchg += "    ELSIF L_DAY_CHECK = 2 THEN`n"
            $programchg += "       DIRLOC := P_SEND_DIR_LOC;`n"
            $programchg += "       L_FILE_ID := SUBSTR(P_PMP_BAT_PGM_ID,5,10)||'_'||P_CED||'.dat';                              /* FILE ID */`n"
            $programchg += "       SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',6400);                                    /* DAT 파일(BUFFER MAX(6400)) */`n"
            $programchg += "       P_PS_CT := 0;`n"
            $programchg += "`n"
            $programchg += "       FOR " + $ExcelTarTable + "_REC IN " + $ExcelTarTable + "2_CUR LOOP"
            $programchg += "`n"
            $programchg += "        SAM_FILE := "
            $programchg += $colDataArr
            $programchg += "        ||'|'`n"
            $programchg += "        ;`n"
            $programchg += "        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);"
            $programchg += "`n"
            $programchg += "        P_PS_CT := P_PS_CT + 1;"
            $programchg += "`n"
            $programchg += "       END LOOP;`n"
            $programchg += "    ELSIF L_DAY_CHECK = 3 THEN`n"
            $programchg += "       DIRLOC := P_SEND_DIR_LOC;`n"
            $programchg += "       L_FILE_ID := SUBSTR(P_PMP_BAT_PGM_ID,5,10)||'_'||P_CED||'.dat';                              /* FILE ID */`n"
            $programchg += "       SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',6400);                                    /* DAT 파일(BUFFER MAX(6400)) */`n"
            $programchg += "       P_PS_CT := 0;`n"
            $programchg += "`n"
            $programchg += "       FOR " + $ExcelTarTable + "_REC IN " + $ExcelTarTable + "3_CUR LOOP"
            $programchg += "`n"
            $programchg += "        SAM_FILE := "
            $programchg += $colDataArr
            $programchg += "        ||'|'`n"
            $programchg += "        ;`n"
            $programchg += "        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);"
            $programchg += "`n"
            $programchg += "        P_PS_CT := P_PS_CT + 1;"
            $programchg += "`n"
            $programchg += "       END LOOP;`n"
            $programchg += "    ELSIF L_DAY_CHECK = 4 THEN`n"
            $programchg += "       DIRLOC := P_SEND_DIR_LOC;`n"
            $programchg += "       L_FILE_ID := SUBSTR(P_PMP_BAT_PGM_ID,5,10)||'_'||P_CED||'.dat';                              /* FILE ID */`n"
            $programchg += "       SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',6400);                                    /* DAT 파일(BUFFER MAX(6400)) */`n"
            $programchg += "       P_PS_CT := 0;`n"
            $programchg += "`n"
            $programchg += "       FOR " + $ExcelTarTable + "_REC IN " + $ExcelTarTable + "4_CUR LOOP"
            $programchg += "`n"
            $programchg += "        SAM_FILE := "
            $programchg += $colDataArr
            $programchg += "        ||'|'`n"
            $programchg += "        ;`n"
            $programchg += "        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);"
            $programchg += "`n"
            $programchg += "        P_PS_CT := P_PS_CT + 1;"
            $programchg += "`n"
            $programchg += "       END LOOP;`n"
            $programchg += "    ELSIF L_DAY_CHECK = 5 THEN`n"
            $programchg += "       DIRLOC := P_SEND_DIR_LOC;`n"
            $programchg += "       L_FILE_ID := SUBSTR(P_PMP_BAT_PGM_ID,5,10)||'_'||P_CED||'.dat';                              /* FILE ID */`n"
            $programchg += "       SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',6400);                                    /* DAT 파일(BUFFER MAX(6400)) */`n"
            $programchg += "       P_PS_CT := 0;`n"
            $programchg += "`n"
            $programchg += "       FOR " + $ExcelTarTable + "_REC IN " + $ExcelTarTable + "5_CUR LOOP"
            $programchg += "`n"
            $programchg += "        SAM_FILE := "
            $programchg += $colDataArr
            $programchg += "        ||'|'`n"
            $programchg += "        ;`n"
            $programchg += "        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);"
            $programchg += "`n"
            $programchg += "        P_PS_CT := P_PS_CT + 1;"
            $programchg += "`n"
            $programchg += "       END LOOP;`n"
            $programchg += "    ELSIF L_DAY_CHECK = 6 THEN`n"
            $programchg += "       DIRLOC := P_SEND_DIR_LOC;`n"
            $programchg += "       L_FILE_ID := SUBSTR(P_PMP_BAT_PGM_ID,5,10)||'_'||P_CED||'.dat';                              /* FILE ID */`n"
            $programchg += "       SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',6400);                                    /* DAT 파일(BUFFER MAX(6400)) */`n"
            $programchg += "       P_PS_CT := 0;`n"
            $programchg += "`n"
            $programchg += "       FOR " + $ExcelTarTable + "_REC IN " + $ExcelTarTable + "6_CUR LOOP"
            $programchg += "`n"
            $programchg += "        SAM_FILE := "
            $programchg += $colDataArr
            $programchg += "        ||'|'`n"
            $programchg += "        ;`n"
            $programchg += "        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);"
            $programchg += "`n"
            $programchg += "        P_PS_CT := P_PS_CT + 1;"
            $programchg += "`n"
            $programchg += "       END LOOP;`n"
            $programchg += "    ELSIF L_DAY_CHECK = 7 THEN`n"
            $programchg += "       DIRLOC := P_SEND_DIR_LOC;`n"
            $programchg += "       L_FILE_ID := SUBSTR(P_PMP_BAT_PGM_ID,5,10)||'_'||P_CED||'.dat';                              /* FILE ID */`n"
            $programchg += "       SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',6400);                                    /* DAT 파일(BUFFER MAX(6400)) */`n"
            $programchg += "       P_PS_CT := 0;`n"
            $programchg += "`n"
            $programchg += "       FOR " + $ExcelTarTable + "_REC IN " + $ExcelTarTable + "7_CUR LOOP"
            $programchg += "`n"
            $programchg += "        SAM_FILE := "
            $programchg += $colDataArr
            $programchg += "        ||'|'`n"
            $programchg += "        ;`n"
            $programchg += "        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);"
            $programchg += "`n"
            $programchg += "        P_PS_CT := P_PS_CT + 1;"
            $programchg += "`n"
            $programchg += "       END LOOP;`n"
            $programchg += "    END IF;`n"
            $programchg += "`n"         
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /*   Sam File close                                                                           */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    UTL_FILE.FCLOSE(SAMFILE_W);`n"
            $programchg += "`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* 최종 로그                                                                                  */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    P_BTJ_ED_F  := 'Y';                                                                             /* 최종작업여부(최종작업인 경우 반드시 기술(예시)) */`n"
            $programchg += "    SP_LOG_MRT(P_PMP_BAT_PGM_ID,P_PMP_BAT_PGM_NM,P_CED,P_BAT_STG_N,P_BAT_STG_NM,P_RM_NI_SR_DT,P_RM_SR_DT,P_PS_CT,P_BAT_ERO_CD,P_BAT_ERO_MSG_TT,P_SSU_CD,P_BTJ_ED_F,P_RTN);`n"
            $programchg += "`n"
            $programchg += "    DBMS_APPLICATION_INFO.SET_ACTION(P_PMP_BAT_PGM_ID||' END');`n"
            $programchg += "`n"
            $programchg += "EXCEPTION`n"
            $programchg += "`n"
            $programchg += "    WHEN USER_EXCEPTION THEN`n"
            $programchg += "    GOTO ERR_RETURN;`n"
            $programchg += "`n"
            $programchg += "    WHEN OTHERS THEN`n"
            $programchg += "    GOTO ERR_RETURN;`n"
            $programchg += "`n"
            $programchg += "    <<ERR_RETURN>>`n"
            $programchg += "`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /*                               SET 적재 LOG 변수(오류)                                      */`n"
            $programchg += "    /*                         ERROR발생시 LOG테이블에 ERROR코드 입력                             */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    IF P_SSU_CD = 'ERR'THEN`n"
            $programchg += "       P_BAT_ERO_CD     := SQLCODE;`n"
            $programchg += "    ELSE`n"
            $programchg += "       P_BAT_ERO_CD     := SQLCODE;`n"
            $programchg += "       P_BAT_ERO_MSG_TT := SQLERRM;`n"
            $programchg += "    END IF;`n"
            $programchg += "`n"
            $programchg += "END;`n"
            
            #$programini | Set-Content -Encoding utf8 "C:\Users\woori\Documents\99.source\추출PROGRAM\마케팅DB추출\변경\UCD_" + $ExcelTarTable + "_TG".sql
            $programchg > C:\Users\woori\Documents\99.source\추출PROGRAM\마케팅DB추출\변경\UCD_$ExcelTarTable"_TG".sql

        } else {
            $programini = "DECLARE`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* 개별변수 선언                                                                              */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    V_CNT       NUMBER(15);                                                                         /* 건수 */`n"
            $programini += "    DIRLOC              VARCHAR2(300)  := NULL;                                                     /* DIR경로 */`n"
            $programini += "    SAMFILE_W           UTL_FILE.FILE_TYPE;                                                         /* SAM WRITE */`n"
            $programini += "    SAM_FILE            VARCHAR2(5000) := NULL;                                                     /* SAM_FILE */`n"
            $programini += "    L_FILE_ID           VARCHAR2(30);   -- FILE ID                                                  /* FILE_ID */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* 추출 DATA  CURSOR                                                                          */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    CURSOR " + $ExcelTarTable + "_CUR IS`n"
            $programini += "    SELECT /*+ PARALLEL(A 16) FULL(A) */`n"
            $programini += $colArr
            $programini += "      FROM " + $ExcelOwner + "." + ($ExcelTable + " A").PadRight(82) + "/* " + $tablenm + " */`n"
            $programini += "    ;`n"
            $programini += "`n"
            $programini += "BEGIN`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* 공통/개별 변수값 Setting                                                                   */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* OWNER, 타겟테이블ID, TEMP테이블ID, PARTITION_ID, 배치프로그램명 등은 PMPHead.sql에서 셋팅  */`n"
            $programini += "    /* 개발 시 추가로 필요한 공통/개별 변수값 Setting 영역                                        */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    V_CNT := 0;                                                                                     /* 건수 개별변수 Setting */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* SAMFILE WRITE                                                                              */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* FILE_ID : TABLE명_YYYYMMDD.dat                                                             */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    P_BAT_STG_N  := LPAD(TO_NUMBER(P_BAT_STG_N)+1,3,'0');                                           /* STEP번호 */`n"
            $programini += "    P_BAT_STG_NM := P_TAR_TBL_ID1 || ' TABLE TRUNCATE';                                             /* STEP명 */`n"
            $programini += "    P_RM_SR_DT   := TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISSFF2');                                   /* STEP시작시간 */`n"
            $programini += "`n"
            $programini += "    DIRLOC := P_SEND_DIR_LOC;`n"
            $programini += "    L_FILE_ID := SUBSTR(P_PMP_BAT_PGM_ID,5,10)||'_'||P_CED||'.dat';                                 /* FILE ID */`n"
            $programini += "    SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',6400);                                       /* DAT 파일(BUFFER MAX(6400)) */`n"
            $programini += "    P_PS_CT := 0;`n"
            $programini += "`n"
            $programini += "    FOR " + $ExcelTarTable + "_REC IN " + $ExcelTarTable + "_CUR LOOP"
            $programini += "`n"
            $programini += "        SAM_FILE := "
            $programini += $colDataArr
            $programini += "        ||'|'`n"
            $programini += "        ;`n"
            $programini += "        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);"
            $programini += "`n"
            $programini += "        P_PS_CT := P_PS_CT + 1;"
            $programini += "`n"
            $programini += "    END LOOP;`n"
            $programini += "`n"         
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /*   Sam File close                                                                           */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    UTL_FILE.FCLOSE(SAMFILE_W);`n"
            $programini += "`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /* 최종 로그                                                                                  */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    P_BTJ_ED_F  := 'Y';                                                                             /* 최종작업여부(최종작업인 경우 반드시 기술(예시)) */`n"
            $programini += "    SP_LOG_MRT(P_PMP_BAT_PGM_ID,P_PMP_BAT_PGM_NM,P_CED,P_BAT_STG_N,P_BAT_STG_NM,P_RM_NI_SR_DT,P_RM_SR_DT,P_PS_CT,P_BAT_ERO_CD,P_BAT_ERO_MSG_TT,P_SSU_CD,P_BTJ_ED_F,P_RTN);`n"
            $programini += "`n"
            $programini += "    DBMS_APPLICATION_INFO.SET_ACTION(P_PMP_BAT_PGM_ID||' END');`n"
            $programini += "`n"
            $programini += "EXCEPTION`n"
            $programini += "`n"
            $programini += "    WHEN USER_EXCEPTION THEN`n"
            $programini += "    GOTO ERR_RETURN;`n"
            $programini += "`n"
            $programini += "    WHEN OTHERS THEN`n"
            $programini += "    GOTO ERR_RETURN;`n"
            $programini += "`n"
            $programini += "    <<ERR_RETURN>>`n"
            $programini += "`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    /*                               SET 적재 LOG 변수(오류)                                      */`n"
            $programini += "    /*                         ERROR발생시 LOG테이블에 ERROR코드 입력                             */`n"
            $programini += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programini += "    IF P_SSU_CD = 'ERR'THEN`n"
            $programini += "       P_BAT_ERO_CD     := SQLCODE;`n"
            $programini += "    ELSE`n"
            $programini += "       P_BAT_ERO_CD     := SQLCODE;`n"
            $programini += "       P_BAT_ERO_MSG_TT := SQLERRM;`n"
            $programini += "    END IF;`n"
            $programini += "`n"
            $programini += "END;`n"

            #$programini | Set-Content -Encoding utf8 "C:\Users\woori\Documents\99.source\추출PROGRAM\마케팅DB추출\초기\UID_" + $ExcelTarTable + "_TG".sql
            $programini > C:\Users\woori\Documents\99.source\추출PROGRAM\마케팅DB추출\초기\UID_$ExcelTarTable"_TG".sql

            $programchg = "DECLARE`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* 개별변수 선언                                                                              */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    V_CNT       NUMBER(15);                                                                         /* 건수 */`n"
            $programchg += "    DIRLOC              VARCHAR2(300)  := NULL;                                                     /* DIR경로 */`n"
            $programchg += "    SAMFILE_W           UTL_FILE.FILE_TYPE;                                                         /* SAM WRITE */`n"
            $programchg += "    SAM_FILE            VARCHAR2(5000) := NULL;                                                     /* SAM_FILE */`n"
            $programchg += "    L_FILE_ID           VARCHAR2(30);   -- FILE ID                                                  /* FILE_ID */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* 추출 DATA  CURSOR                                                                          */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    CURSOR " + $ExcelTarTable + "_CUR IS`n"
            $programchg += "    SELECT /*+ PARALLEL(A 16) FULL(A) */`n"
            $programchg += $colArr
            $programchg += "      FROM " + $ExcelOwner + "." + ($ExcelTable + " A").PadRight(82) + "/* " + $tablenm + " */`n"
            $programchg += "     WHERE " + $ExcelChgTable + " >= P_CED`n"
            $programchg += "    ;`n"
            $programchg += "`n"
            $programchg += "BEGIN`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* 공통/개별 변수값 Setting                                                                   */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* OWNER, 타겟테이블ID, TEMP테이블ID, PARTITION_ID, 배치프로그램명 등은 PMPHead.sql에서 셋팅  */`n"
            $programchg += "    /* 개발 시 추가로 필요한 공통/개별 변수값 Setting 영역                                        */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    V_CNT := 0;                                                                                     /* 건수 개별변수 Setting */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* SAMFILE WRITE                                                                              */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* FILE_ID : TABLE명_YYYYMMDD.dat                                                             */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    P_BAT_STG_N  := LPAD(TO_NUMBER(P_BAT_STG_N)+1,3,'0');                                           /* STEP번호 */`n"
            $programchg += "    P_BAT_STG_NM := P_TAR_TBL_ID1 || ' TABLE TRUNCATE';                                             /* STEP명 */`n"
            $programchg += "    P_RM_SR_DT   := TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISSFF2');                                   /* STEP시작시간 */`n"
            $programchg += "`n"
            $programchg += "    DIRLOC := P_SEND_DIR_LOC;`n"
            $programchg += "    L_FILE_ID := SUBSTR(P_PMP_BAT_PGM_ID,5,10)||'_'||P_CED||'.dat';                                 /* FILE ID */`n"
            $programchg += "    SAMFILE_W := UTL_FILE.FOPEN(DIRLOC, L_FILE_ID, 'W',6400);                                       /* DAT 파일(BUFFER MAX(6400)) */`n"
            $programchg += "    P_PS_CT := 0;`n"
            $programchg += "`n"
            $programchg += "    FOR " + $ExcelTarTable + "_REC IN " + $ExcelTarTable + "_CUR LOOP"
            $programchg += "`n"
            $programchg += "        SAM_FILE := "
            $programchg += $colDataArr
            $programchg += "        ||'|'`n"
            $programchg += "        ;`n"
            $programchg += "        UTL_FILE.PUT_LINE(SAMFILE_W, SAM_FILE);"
            $programchg += "`n"
            $programchg += "        P_PS_CT := P_PS_CT + 1;"
            $programchg += "`n"
            $programchg += "    END LOOP;`n"
            $programchg += "`n"         
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /*   Sam File close                                                                           */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    UTL_FILE.FCLOSE(SAMFILE_W);`n"
            $programchg += "`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /* 최종 로그                                                                                  */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    P_BTJ_ED_F  := 'Y';                                                                             /* 최종작업여부(최종작업인 경우 반드시 기술(예시)) */`n"
            $programchg += "    SP_LOG_MRT(P_PMP_BAT_PGM_ID,P_PMP_BAT_PGM_NM,P_CED,P_BAT_STG_N,P_BAT_STG_NM,P_RM_NI_SR_DT,P_RM_SR_DT,P_PS_CT,P_BAT_ERO_CD,P_BAT_ERO_MSG_TT,P_SSU_CD,P_BTJ_ED_F,P_RTN);`n"
            $programchg += "`n"
            $programchg += "    DBMS_APPLICATION_INFO.SET_ACTION(P_PMP_BAT_PGM_ID||' END');`n"
            $programchg += "`n"
            $programchg += "EXCEPTION`n"
            $programchg += "`n"
            $programchg += "    WHEN USER_EXCEPTION THEN`n"
            $programchg += "    GOTO ERR_RETURN;`n"
            $programchg += "`n"
            $programchg += "    WHEN OTHERS THEN`n"
            $programchg += "    GOTO ERR_RETURN;`n"
            $programchg += "`n"
            $programchg += "    <<ERR_RETURN>>`n"
            $programchg += "`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    /*                               SET 적재 LOG 변수(오류)                                      */`n"
            $programchg += "    /*                         ERROR발생시 LOG테이블에 ERROR코드 입력                             */`n"
            $programchg += "    /*--------------------------------------------------------------------------------------------*/`n"
            $programchg += "    IF P_SSU_CD = 'ERR'THEN`n"
            $programchg += "       P_BAT_ERO_CD     := SQLCODE;`n"
            $programchg += "    ELSE`n"
            $programchg += "       P_BAT_ERO_CD     := SQLCODE;`n"
            $programchg += "       P_BAT_ERO_MSG_TT := SQLERRM;`n"
            $programchg += "    END IF;`n"
            $programchg += "`n"
            $programchg += "END;`n"

            #$programchg | Set-Content -Encoding utf8 "C:\Users\woori\Documents\99.source\추출PROGRAM\마케팅DB추출\변경\UCD_" + $ExcelTarTable + "_TG.sql"
            $programchg > C:\Users\woori\Documents\99.source\추출PROGRAM\마케팅DB추출\변경\UCD_$ExcelTarTable"_TG".sql
        }
     }
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