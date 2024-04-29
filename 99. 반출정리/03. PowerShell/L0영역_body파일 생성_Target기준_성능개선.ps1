Add-Type -AssemblyName System.Data.OracleClient

$PSDefaultParameterValues['*:Encoding'] = 'Default'
#$PSDefaultParameterValues['*:Encoding'] = 'UTF8'

$utf8NoBomEncoding = New-Object System.Text.UTF8Encoding($False)

# 운영계 통합운영정보
$driver = "MySQL ODBC 8.0 ANSI Driver"
$server = "dept-rds-an2-op-dev-opmdsdbd.cluster-c92dc3uwdt0o.ap-northeast-2.rds.amazonaws.com"
$username = "MDSDEV"
$password = "md`$D2v2023"
$data_source = "MDSDEV"
#$connection_string = "User Id=$username;Password=$password;Data Source=$data_source"
$connectionString = "DRIVER={$driver};Server=$server;Port=3306;Database=$data_source;Uid=$username;Pwd=$password"

# 운영계 STG
<#$driver = "MySQL ODBC 8.0 ANSI Driver`
$server = "dept-rds-an2-op-stg-opmdsdbt.cluster-ro-ckcic5dtfarb.ap-northeast-2.rds.amazonaws.com"
$username = "ANFED"
$password = "d55!43ED"
$data_source = "MDSSTG"
#$connection_string = "User Id=$username;Password=$password;Data Source=$data_source"
$connectionString = "DRIVER={$driver};Server=$server;Port=3306;Database=$data_source;Uid=$username;Pwd=$password"
#>
# 분석계 개발DB(Redshift)
$redhost = "dept-rsc-an2-cm-dwdev-ansla.ctdnukmkv08j.ap-northeast-2.redshift.amazonaws.com"
$redport = 5439
$reddbName = "bludbev"
$redusername = "etluser08"
$redpassword = "!Etluser08"
$redconnectionString = "Driver={Amazon Redshift (x64)}; Server=$redhost; Port=$redport; Database=$reddbName; UID=$redusername; PWD=$redpassword;"

$startTime = (Get-Date)

try {
     #작업 목록
     $ExcelObj = new-Object -Comobject Excel.Application
     $ExcelObj.visible=$false

     $ExcelWorkBook = $ExcelObj.Workbooks.Open("C:\Users\Owner\Desktop\신세계백화점 차세대 시스템 구축\03. 개발\99. 관리문서\SSGDX_A_AN_TOBE-SOR 테이블 목록.xlsx")
     #$ExcelWorkSheet = $ExcelWorkBook.Sheets.Item("IF 목록")
     $ExcelWorkSheet = $ExcelWorkBook.Sheets.Item("SOR 목록")
     #$ExcelWorkSheet = $ExcelWorkBook.Sheets.Item("test")

     $rowcount=$ExcelWorkSheet.UsedRange.Rows.Count
     
     #제외 대상 컬럼 목록
     #$ExcelObj1 = new-Object -Comobject Excel.Application
     #$ExcelObj1.visible=$false

     #$ExcelWorkBook1 = $ExcelObj1.Workbooks.Open("C:\Users\Owner\Desktop\신세계백화점 차세대 시스템 구축\03. 개발\99. 관리문서\SSGDX_A_AN_설계준비(컬럼삭제대상컬럼목록).xlsx")
     #$ExcelWorkSheet1 = $ExcelWorkBook1.Sheets.Item("Sheet1")
     
     #$rowcount1=$ExcelWorkSheet1.UsedRange.Rows.Count
     
     $dsjoball=@()

     for ($i=2; $i -le $rowcount; $i++) { 
         $Exceljuje = $ExcelWorkSheet.cells.Item($i, 2).value2
         $Excelasisowner = $ExcelWorkSheet.cells.Item($i, 3).value2
         $Excelasistable = $ExcelWorkSheet.cells.Item($i, 4).value2
         $Excelasistbnm = $ExcelWorkSheet.cells.Item($i, 5).value2
         $ExcelOwner = $ExcelWorkSheet.cells.Item($i, 6).value2
         $ExcelTable = $ExcelWorkSheet.cells.Item($i, 7).value2
         $ExcelTableNm = $ExcelWorkSheet.cells.Item($i, 8).value2
         $Excelchgyn = $ExcelWorkSheet.cells.Item($i, 9).value2
         $Excelchgkey = $ExcelWorkSheet.cells.Item($i, 10).value2
         $Excelfolder = $ExcelWorkSheet.cells.Item($i, 11).value2
         $Excelbase = $ExcelWorkSheet.cells.Item($i, 12).value2

         if ($Exceljuje -eq "17.재무관리") {
             $Exceljuje = "SAP"
         }

         if ($Excelasistable -eq "X") {
             continue
         } else {
             $ExternalTable = $Excelasistable
             $TableLength = $ExcelTable.Length-1
             if($Exceljuje -eq "SAP" -or $Exceljuje -eq "SCRM") {
                 $TempTable = $Excelasistable
             } else {
                 $TempTable = $ExcelTable.Substring(1, $TableLength)
             }
             $TempOwner = "ANSTG"
         }
         Write-Host "--------------------" $ExcelOwner"."$ExcelTable

         $deletecolarr = @() # 변경적재용 delete sql
         $tempinsertarr = @()
         $tarinsertarr = @()
         $truncatearr = @()
         $truncatetmparr = @()
         $snpdeletecolarr = @()
         $snptarinsertarr = @()
         $deletepk = @()
         $colarr = @()
         $etlcolarr = @()
         $selcolarr = @()
         $tempselcolarr = @()
         $tempinscolarr = @()
         $columnidset = ""
         $bodysql = @()
         $bodyuidsql = @()
         $bodyupdatesql = @()
         $bodyimplesql = @()
         $joinarr = @()
         $setarr = @()
         $pkchk = 100
         $setposition = 0
         $impleChk = "N"
         $chgKey = ""
         $betweenarr = ""
         $implbetweenarr = ""
         $sapprocom = ""
         $pgmLCDid = ""
         $pgmLIDid = ""
         
         if ($Exceljuje -eq "SAP" -or $Exceljuje -eq "SCRM") {
             $Excelsapowner = $ExcelOwner.ToLower()
             $Excelsaptable = $ExcelTable.ToLower()
         $redstatement = "SELECT T10.TABLE_SCHEMA
`                               , T10.TABLE_NAME
`                               , COALESCE(obj_description('" + $ExcelsapOwner + "." + $Excelsaptable + "'::regclass), '없음') AS table_comment
`                               , T20.COLUMN_NAME
`                               , COALESCE(T40.description::varchar, '없음')
`                               , T20.ORDINAL_POSITION
`                               , COALESCE(CASE WHEN T20.DATA_TYPE = 'character varying' or T20.DATA_TYPE = 'character'
`                                      THEN 'VARCHAR(' || T20.CHARACTER_MAXIMUM_LENGTH || ')'
`                                      WHEN T20.DATA_TYPE = 'smallint'
`                                      THEN 'INTEGER(' || T20.NUMERIC_PRECISION || ')'
`                                      ELSE T20.DATA_TYPE
`                                  END,'없음')
`                               , T20.IS_NULLABLE
`                               , COALESCE(T20.COLUMN_DEFAULT,'없음')
`                               , T20.DATA_TYPE
`                               , COALESCE(T20.NUMERIC_PRECISION,-999)
`                               , COALESCE(T20.NUMERIC_SCALE,0)
`                               , COALESCE(T20.CHARACTER_MAXIMUM_LENGTH,0)
`                            FROM INFORMATION_SCHEMA.TABLES AS T10
`                            LEFT
`                            JOIN INFORMATION_SCHEMA.COLUMNS AS T20
`                              ON T10.TABLE_SCHEMA = T20.TABLE_SCHEMA
`                             AND T10.TABLE_NAME = T20.TABLE_NAME
`                            LEFT 
`                            JOIN pg_catalog.pg_stat_all_tables as T30
`                              ON T20.TABLE_SCHEMA = T30.schemaname
`                             AND T20.TABLE_NAME = T30.relname
`                            LEFT 
`                            JOIN pg_catalog.pg_description as T40
`                              ON T30.relid = T40.objoid
`                             and T20.ORDINAL_POSITION = T40.objsubid
`                             AND T40.objsubid <> 0
`                           WHERE T10.TABLE_SCHEMA = '" + $ExcelsapOwner + "'
`                             AND T10.TABLE_NAME = '" + $Excelsaptable + "'
`                           ORDER BY T10.TABLE_NAME, T20.ORDINAL_POSITION
`;"

             # SAP 레이아웃을 불러오기 위한 분석계 개발DB 연결
             $redconnection = New-Object System.Data.Odbc.OdbcConnection($redconnectionString)
             $redconnection.Open()

    
             $redcommand = New-Object System.Data.Odbc.OdbcCommand($redstatement, $redconnection)
             $redcommand.CommandText = $redstatement
             $redresult = $redcommand.ExecuteReader()

             if($ExcelTable -eq "SFM_ZACT1530" -or $ExcelTable -eq "SFM_ZPCT0001" -or $ExcelTable -eq "SFM_ZPCT0002" -or $ExcelTable -eq "SFM_ZACS2020") {
                 $sapprocom = "IGNOREHEADER 1 ENCODING UTF8"
             } else {
                 $sapprocom = "IGNOREHEADER 1 BLANKSASNULL EMPTYASNULL ENCODING UTF8"
             }

             if($ExcelTable -eq "SCI_CUST_CLUB") {
                 $scrmprocom = "ACCEPTINVCHARS ESCAPE ENCODING UTF8"
             } else {
                 $scrmprocom = "ACCEPTINVCHARS ESCAPE BLANKSASNULL EMPTYASNULL ENCODING UTF8"
             }

             while ($redresult.Read())
             {
              $Owner = $redresult.GetString(0)
              $tableid = $redresult.GetString(1)
              $tablenm = $redresult.GetString(2)
              $columnid = $redresult.GetString(3).ToUpper()
              $columnnm = $redresult.GetString(4)
              $position = $redresult.GetValue(5)
              $columntype = $redresult.GetValue(6)
              $nullable = $redresult.GetString(7)
              $default = $redresult.GetString(8)
              $datatype = $redresult.GetString(9)
              $precision = $redresult.GetString(10)
              $scale = $redresult.GetString(11)
              $maxlength = $redresult.GetString(12)

              $pkyn = '1'

              if ($columnid -eq "ETL_WRK_DTM") {
                  $pkchk = $position
                  $setposition = $setposition + 1
              }

              if ($position -lt $pkchk) {
                  $pkyn = '1'
              } else {
                  $pkyn = '0'
              }
    
              if ($columnid -eq 'CD_DES' -or $columnid -eq 'CD_SST_DES') {
                  $columnidset = "REPLACE(REPLACE(" + $columnid + ", '\\r\\n', ''), '\\n', '') AS " + $columnid
              } else {
                  $columnidset = $columnid
              }
    
              # Delete 용 pk
              if ($pkyn -eq '1' -and $position -eq 1) {
                  $deletepk += "                WHERE " + $ExcelTable + "." + $columnid + " = " + $TempTable + "." + $columnid + "`n"
              } elseif ($pkyn -eq '1') {
                  $deletepk += "                 AND " + $ExcelTable + "." + $columnid + " = " + $TempTable + "." + $columnid + "`n"
              }

              # ZGLITEMS02 일경우 ZGLITEMS01 테이블 JOIN UPDATE를 위한 JOIN 조건
              if ($tableid -eq "SFM_ZGLITEMS02") {
                  if ($pkyn -eq '1' -and $position -eq 1) {
                      $joinarr += "    ON T10." + $columnid + " = T11." + $columnid + "`n"
                  } elseif ($pkyn -eq '1') {
                      $joinarr += "  AND T10." + $columnid + " = T11." + $columnid + "`n"
                  } else {
                      if ($columnid -eq "ETL_WRK_DTM") {
                          
                      } elseif ($setposition -eq 1) {
                          $setarr += $columnid + " = T11." + $columnid + "`n"
                          $setposition = 0
                      } else {
                          $setarr += "    , " + $columnid + " = T11." + $columnid + "`n"
                      }
                  }
              }

              # insert 용 Column Array
              if ($position -eq 1) {
                  $colarr += "       " + $columnid.PadRight(93) + "/* " + $columnnm + " */`n"
              } else {
                  $colarr += "    , " + $columnid.PadRight(93) + "/* " + $columnnm + " */`n"
              }

              # insert-select 용 Column Array
              if ($position -eq 1) {
                  $selcolarr += "       " + $columnidset.PadRight(93) + "/* " + $columnnm + " */`n"
              } elseif ($columnidset -eq "ETL_WRK_DTM") {
                  #$selcolarr += "    , " + "etl_wrk_dtm".PadRight(93) + "/* " + "ETL작업일시" + " */`n"
                  $selcolarr += "    , CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'".PadRight(99) + "/* " + $columnnm + " */`n"
              } else {
                  $selcolarr += "    , " + $columnidset.PadRight(93) + "/* " + $columnnm + " */`n"
              }

              # Target-Staging Delete 문 생성
              if ($ExcelTable -eq "SFM_ZACS2020") {
                  $deletecolarr = 
"DELETE /* " + $ExcelTable + "(LCD_" + $ExcelTable + "_TG) */
`  FROM " + $ExcelOwner + "." + $ExcelTable.PadRight(87) + "/* " + $ExcelTableNm + " */
` WHERE EXISTS (
`               SELECT 1
`                 FROM " + $TempOwner + "." + $TempTable + " T20
`                WHERE T20.BUNIT = " + $ExcelTable + ".BUNIT
`                  AND T20.PL_FORM = " + $ExcelTable + ".PL_FORM
`                  AND T20.PERIO = " + $ExcelTable + ".PERIO
`              )"
                  $truncatearr = "TRUNCATE /* " + ($ExcelTable + "(LCD_" + $ExcelTable + "_TG) */ " + $ExcelOwner + "." + $ExcelTable).PadRight(88) + "/* " + $ExcelTableNm + " */"
                  $truncatetmparr = "TRUNCATE /* " + ($ExcelTable + "(LCD_" + $ExcelTable + "_TG) */ " + $TempOwner + "." + $TempTable).PadRight(88) + "/* " + $ExcelTableNm + " */"
              } elseif ($Excelchgyn -eq 'Y') {
                  $deletecolarr = "DELETE /* " + $ExcelTable + "(LCD_" + $ExcelTable + "_TG) */`n  FROM " + $ExcelOwner + "." + $ExcelTable.PadRight(87) + "/* " + $ExcelTableNm + " */`n WHERE EXISTS (`n               SELECT 1`n                 FROM " + $TempOwner + "." + $TempTable.PadRight(72) + "/* " + $ExcelTableNm + " */`n" + $deletepk + "              )"
                  $truncatearr = "TRUNCATE /* " + ($ExcelTable + "(LCD_" + $ExcelTable + "_TG) */ " + $ExcelOwner + "." + $ExcelTable).PadRight(88) + "/* " + $ExcelTableNm + " */"
                  $truncatetmparr = "TRUNCATE /* " + ($ExcelTable + "(LCD_" + $ExcelTable + "_TG) */ " + $TempOwner + "." + $TempTable).PadRight(88) + "/* " + $ExcelTableNm + " */"
              } else {
                  $deletecolarr = "DELETE /* " + $ExcelTable + "(LCD_" + $ExcelTable + "_TG) */`n  FROM " + $ExcelOwner + "." + $ExcelTable.PadRight(87) + "/* " + $ExcelTableNm + " */"
                  $truncatearr = "TRUNCATE /* " + ($ExcelTable + "(LCD_" + $ExcelTable + "_TG) */ " + $ExcelOwner + "." + $ExcelTable).PadRight(88) + "/* " + $ExcelTableNm + " */"
                  $truncatetmparr = "TRUNCATE /* " + ($ExcelTable + "(LCD_" + $ExcelTable + "_TG) */ " + $TempOwner + "." + $TempTable).PadRight(88) + "/* " + $ExcelTableNm + " */"
              }

              # Target 적재 INSERT 문 생성
              $tarinsertarr = "INSERT /* " + $ExcelTable + "(LCD_" + $ExcelTable + "_TG) */`n  INTO " + $ExcelOwner + "." + $ExcelTable.PadRight(87) + "/* " + $ExcelTableNm + " */`n     (`n" + $colarr + "     )`nSELECT /* " + $ExcelTable + "(LCD_" + $ExcelTable + "_TG) */`n" + $selcolarr + "  FROM " + $TempOwner + "." + $TempTable.PadRight(87) + "/* " + $ExcelTableNm + " */"

              # ZGLITEMS02 일경우 ZGLITEMS01 테이블 JOIN UPDATE를 위한 UPDATE문
              if ($tableid -eq "SFM_ZGLITEMS02") {
                  $updatearr = "UPDATE /* " + $ExcelTable + "(LCD_" + $ExcelTable + "_TG) */ ANSOR.SFM_ZGLITEMS01`n   SET " + $setarr + "  FROM ANSOR.SFM_ZGLITEMS01 T10 `n INNER JOIN ANSOR.SFM_ZGLITEMS02 T11 `n" + $joinarr
              }

             } # SAP While 끝

         } else {
             if ($Excelchgyn -eq 'S') {
                 $Excelredowner = $Excelasisowner.ToLower()
                 $Excelredtable = $Excelasistable.ToLower()
             } else {
                 $Excelredowner = $ExcelOwner.ToLower()
                 $Excelredtable = $ExcelTable.ToLower()
             }
         $statement = "SELECT T10.TABLE_SCHEMA
`                               , T10.TABLE_NAME
`                               , COALESCE(obj_description('" + $Excelredowner + "." + $Excelredtable + "'::regclass), '없음') AS table_comment
`                               , T20.COLUMN_NAME
`                               , COALESCE(T40.description::varchar, '없음')
`                               , T20.ORDINAL_POSITION
`                               , COALESCE(CASE WHEN T20.DATA_TYPE = 'character varying' or T20.DATA_TYPE = 'character'
`                                      THEN 'VARCHAR(' || T20.CHARACTER_MAXIMUM_LENGTH || ')'
`                                      WHEN T20.DATA_TYPE = 'smallint'
`                                      THEN 'INTEGER(' || T20.NUMERIC_PRECISION || ')'
`                                      ELSE T20.DATA_TYPE
`                                  END,'없음')
`                               , T20.IS_NULLABLE
`                               , COALESCE(T20.COLUMN_DEFAULT,'없음')
`                               , T20.DATA_TYPE
`                               , COALESCE(T20.NUMERIC_PRECISION,-999)
`                               , COALESCE(T20.NUMERIC_SCALE,0)
`                               , COALESCE(T20.CHARACTER_MAXIMUM_LENGTH,0)
`                            FROM INFORMATION_SCHEMA.TABLES AS T10
`                            LEFT
`                            JOIN INFORMATION_SCHEMA.COLUMNS AS T20
`                              ON T10.TABLE_SCHEMA = T20.TABLE_SCHEMA
`                             AND T10.TABLE_NAME = T20.TABLE_NAME
`                            LEFT 
`                            JOIN pg_catalog.pg_stat_all_tables as T30
`                              ON T20.TABLE_SCHEMA = T30.schemaname
`                             AND T20.TABLE_NAME = T30.relname
`                            LEFT 
`                            JOIN pg_catalog.pg_description as T40
`                              ON T30.relid = T40.objoid
`                             and T20.ORDINAL_POSITION = T40.objsubid
`                             AND T40.objsubid <> 0
`                           WHERE T10.TABLE_SCHEMA = '" + $Excelredowner + "'
`                             AND T10.TABLE_NAME = '" + $Excelredtable + "'
`                           ORDER BY T10.TABLE_NAME, T20.ORDINAL_POSITION
`;"

             if ($Excelchgyn -eq "Y") { #-and $Excelchgkey -ne "UPD_DTM"
                 $impleChk = "Y"
                 $chgKey = $Excelchgkey
             }
             if ($tableid -eq "SIV_TDMR_BYDY_MD_S" -or $tableid -eq "SIV_TDMR_BYMN_MD_S" -or $tableid -eq "SPF_SALS_ACR_YMD" -or $tableid -eq "SPF_ORD_ACR_YMD" -or $tableid -eq "SSS_MNG_PRLS_SALS_P") {
                 $LCDchgKey = $Excelchgkey
             } else {
                 $LCDchgKey = "UPD_DTM"
             }

             if ($impleChk -eq "Y") {
                 $pgmLIDid = "LID_" + $ExcelTable + "_TG"
                 $pgmLCDid = "LCD_" + $ExcelTable + "_TG"
             } else {
                 $pgmLCDid = "LCD_" + $ExcelTable + "_TG"
             }

             if ($Excelchgkey -eq "UPD_DTM") {
                 #$betweenarr = " BETWEEN TO_TIMESTAMP('{BASE_DT_FROM}' || '000000', 'YYYYMMDDHH24MISS') AND TO_TIMESTAMP('{BASE_DT_TO}' || '235999', 'YYYYMMDDHH24MISS')"
                 $betweenarr = " >= TO_TIMESTAMP('{BASE_DT_TO}' || '000000', 'YYYYMMDDHH24MISS')"
             } else {
                 #$betweenarr = " BETWEEN '{BASE_DT_FROM}' AND '{BASE_DT_TO}'"
                 #$betweenarr = " >= '{BASE_DT_TO}'"
                 if ($tableid -eq "SIV_TDMR_BYMN_MD_S") {
                     $betweenarr = " BETWEEN TO_CHAR(ADD_MONTHS(TO_DATE('{BASE_DT_TO}','YYYYMMDD'),-2),'YYYYMM') AND '{BASE_YM_TO}'"
                 } elseif ($chgKey -ne "" -and $chgKey.Substring($Excelchgkey.Length -3) -eq "_YM"){
                     $betweenarr = " >= '{BASE_YM_TO}'"
                 } elseif ($tableid -eq "SIV_TDMR_BYDY_MD_S" -or $tableid -eq "SSS_MNG_PRLS_SALS_P") {
                     $betweenarr = " BETWEEN TO_CHAR(ADD_MONTHS(TO_DATE('{BASE_YM_TO}','YYYYMM'),-2),'YYYYMM') || '01' AND TO_CHAR(LAST_DAY(TO_DATE('{BASE_DT_TO}','YYYYMMDD')), 'YYYYMMDD')"
                     ## 2024.01.31 미래데이터 발생으로 인한 로직 수정, PARAMETER 에 대한 말일자 계산 로직 추가
                     ## 미래데이터가 월을 넘어가는경우 발생시 해당 로직 재수정 ADD_MONTHS + 1 ==> TO_CHAR(ADD_MONTHS(LAST_DAY(TO_DATE('{BASE_DT_TO}','YYYYMMDD')),1), 'YYYYMMDD')
                 } elseif ($tableid -eq "SPF_SALS_ACR_YMD" -or $tableid -eq "SPF_ORD_ACR_YMD") {
                     $betweenarr = " BETWEEN '{BASE_DT_FROM}' AND '{BASE_DT_TO}'"
                     ## 2024.02.19 SPF_매출_실적일자, SPF_주문_실적일자 테이블 예외사항 추가

                 } else {
                     $betweenarr = " >= TO_TIMESTAMP('{BASE_DT_TO}' || '000000', 'YYYYMMDDHH24MISS')"
                 }
             }

             if ($chgKey -eq "UPD_DTM") {
                 $implbetweenarr = " BETWEEN TO_TIMESTAMP('{BASE_DT_FROM}' || '000000', 'YYYYMMDDHH24MISS') AND TO_TIMESTAMP('{BASE_DT_TO}' || '235999', 'YYYYMMDDHH24MISS')"
                 #$implbetweenarr = " >= TO_TIMESTAMP('{BASE_DT_TO}' || '000000', 'YYYYMMDDHH24MISS')"
             } elseif($chgKey -eq "") {

             } else {
                 if ($chgKey.Substring($chgKey.Length -3) -eq "_YM"){
                     $implbetweenarr = " BETWEEN '{BASE_YM_FROM}' AND '{BASE_YM_TO}'"
                     #$implbetweenarr = " >= '{BASE_YM_TO}'"
                 } else {
                     $implbetweenarr = " BETWEEN '{BASE_DT_FROM}' AND '{BASE_DT_TO}'"
                     #$implbetweenarr = " >= '{BASE_DT_TO}'"
                 }
             }

             # 분석계 레이아웃을 불러오기 위한 redshift 연결
             $redconnection = New-Object System.Data.Odbc.OdbcConnection($redconnectionString)
             $redconnection.Open()

    
             $command = New-Object System.Data.Odbc.OdbcCommand($statement, $redconnection)
             $command.CommandText = $statement
             $result = $command.ExecuteReader()
    
             while ($result.Read())
             {
              $Owner = $result.GetString(0)
              $tableid = $result.GetString(1)
              $tablename = $result.GetString(2)
              $columnid = $result.GetString(3).ToUpper()
              $columnname = $result.GetString(4)
              $position = $result.GetValue(5)
              $columntype = $result.GetString(6)
              $nullable = $result.GetString(7)
              $default = $result.GetString(8)
              $datatype = $result.GetString(9)
              $precision = $result.GetString(10)
              $scale = $result.GetString(11)
              $maxlength = $result.GetString(12)

              if ($columnid -eq "X") {
                  continue
              }

              $pkyn = '1'

              if ($columnid -eq "ETL_WRK_DTM") {
                  $pkchk = $position
              }

              if ($position -lt $pkchk) {
                  $pkyn = '1'
              } else {
                  $pkyn = '0'
              }
    
              if ($columnid -eq 'CD_DES' -or $columnid -eq 'CD_SST_DES') {
                  $columnidset = "REPLACE(REPLACE(" + $columnid + ", '\\r\\n', ''), '\\n', '') AS " + $columnid
              } else {
                  $columnidset = $columnid
              }
    
              # Delete 용 pk
              if ($pkyn -eq '1' -and $position -eq 1) {
                  $deletepk += "                WHERE " + $ExcelTable + "." + $columnid + " = " + $TempTable + "." + $columnid + "`n"
              } elseif ($pkyn -eq '1') {
                  $deletepk += "                 AND " + $ExcelTable + "." + $columnid + " = " + $TempTable + "." + $columnid + "`n"
              }

    
              # target insert 용 Column Array
              if ($Excelchgyn -eq 'S' -and $position -eq 1) {
                  $colarr += "       CRI_YMD".PadRight(100) + "/* 기준일자 */`n"
                  $colarr += "    , " + $columnid.PadRight(93) + "/* " + $columnnm + " */`n"
              } elseif ($position -eq 1) {
                  $colarr += "       " + $columnid.PadRight(93) + "/* " + $columnname + " */`n"
              } elseif ($columnid -eq "REG_DTM") {
                  #$colarr += "    , " + "etl_wrk_dtm".PadRight(100) + "/* " + "ETL작업일시" + " */`n"
                  $colarr += "    , " + $columnid.PadRight(93) + "/* " + $columnname + " */`n"
              } else {
                  $colarr += "    , " + $columnid.PadRight(93) + "/* " + $columnname + " */`n"
              }

              #ETL 적재일시를 넣기위한 로직
              if ($Excelchgyn -eq 'S' -and $position -eq 1) {
                  $etlcolarr += "       '{BASE_DT_TO}'".PadRight(100) + "/* 기준일자 */`n" # 기준일자에 들어갈 값 정해지면 바꾸기
                  $etlcolarr += "    , " + $columnidset.PadRight(93) + "/* " + $columnnm + " */`n"
              } elseif ($position -eq 1) {
                  $etlcolarr += "       " + $columnidset.PadRight(93) + "/* " + $columnname + " */`n"
              } elseif ($columnid -eq "REG_DTM") {
                  #$etlcolarr += "    , " + "to_timestamp(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul','YYYY-MM-DD HH24:MI:SS')".PadRight(100) + "/* " + "ETL작업일시" + " */`n"
                  $etlcolarr += "    , " + $columnidset.PadRight(93) + "/* " + $columnname + " */`n"
              } elseif ($columnid -eq "ETL_WRK_DTM") {
                  $etlcolarr += "    , " + "CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'".PadRight(93) + "/* " + "ETL작업일시" + " */`n"
                  #$etlcolarr += "    , " + $columnidset.PadRight(100) + "/* " + $columnname + " */`n"
              } else {
                  $etlcolarr += "    , " + $columnidset.PadRight(93) + "/* " + $columnname + " */`n"
              }

              # temp insert-select 용 Column Array
              if ($position -eq 1) {
                  $selcolarr += "       " + $columnidset.PadRight(100) + "/* " + $columnname + " */`n"
              } else {
                  $selcolarr += "    , " + $columnidset.PadRight(100) + "/* " + $columnname + " */`n"
              }

              # temp insert-select 용 Column Array
              if ($columnid -eq "ETL_WRK_DTM") {
                  $tempinscolarr += "    , " + "CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'".PadRight(93) + "/* " + $columnname + " */`n"
                  $tempselcolarr += "    , " + $columnidset.PadRight(93) + "/* " + $columnname + " */`n"
              } else {
                  if ($position -eq 1) {
                      $tempinscolarr += "       `"" + ($columnid.ToUpper() + "`"").PadRight(92) + "/* " + $columnname + " */`n"
                      $tempselcolarr += "       " + $columnid.PadRight(93) + "/* " + $columnname + " */`n"
                  } else {
                      $tempinscolarr += "    , `"" + ($columnid.ToUpper() + "`"").PadRight(92) + "/* " + $columnname + " */`n"
                      $tempselcolarr += "    , " + $columnid.PadRight(93) + "/* " + $columnname + " */`n"
                  } 
              }    
             } #While문 끝
             
             # Target-Staging Delete 문 생성
             if ($Excelchgyn -eq 'S') {
                 $snpdeletecolarr = "DELETE /* " + $ExcelTable + "(" + $pgmLCDid + ") */`n  FROM " + $ExcelOwner + "." + $ExcelTable.PadRight(87) + "/* " + $ExcelTableNm + " */`n WHERE CRI_YMD = '{BASE_DT_TO}'"
                 
                 $snptarinsertarr = "INSERT /* " + $ExcelTable + "(" + $pgmLCDid + ") */`n  INTO " + $ExcelOwner + "." + $ExcelTable.PadRight(87) + "/* " + $ExcelTableNm + " */`n     (`n" + $colarr + "     )`nSELECT`n" + $etlcolarr + "  FROM " + $ExcelOwner + "." + $Excelasistable.PadRight(87) + "/* " + $ExcelTableNm + " */"
             } else { 
                 if ($Excelchgyn -eq 'Y') {
                     if ($tableid -eq "SIV_TDMR_BYDY_MD_S" -or $tableid -eq "SIV_TDMR_BYMN_MD_S" -or $tableid -eq "SPF_SALS_ACR_YMD" -or $tableid -eq "SPF_ORD_ACR_YMD") {
                         $deletecolarr = "DELETE /* " + $ExcelTable + "(" + $pgmLCDid + ") */`n  FROM " + ($ExcelOwner + "." + $ExcelTable).PadRight(93) + "/* " + $ExcelTableNm + " */`n WHERE " + $LCDchgKey + $betweenarr
                     } else {
                         $deletecolarr = "DELETE /* " + $ExcelTable + "(" + $pgmLCDid + ") */`n  FROM " + ($ExcelOwner + "." + $ExcelTable).PadRight(93) + "/* " + $ExcelTableNm + " */`n WHERE EXISTS (`n               SELECT 1`n                 FROM " + $TempOwner + "." + $TempTable.PadRight(72) + "/* " + $ExcelTableNm + " */`n" + $deletepk + "              )"
                     }
                     $truncatearr = "TRUNCATE /* " + ($ExcelTable + "(" + $pgmLCDid + ") */" + $ExcelOwner + "." + $ExcelTable).PadRight(88) + "/* " + $ExcelTableNm + " */"
                     $truncatetmparr = "TRUNCATE /* " + ($ExcelTable + "(LCD_" + $ExcelTable + "_TG) */ " + $TempOwner + "." + $TempTable).PadRight(88) + "/* " + $ExcelTableNm + " */"
                     $impldeletecolarr = "DELETE /* " + $ExcelTable + "(" + $pgmLIDid + ") */`n  FROM " + ($ExcelOwner + "." + $ExcelTable).PadRight(93) + "/* " + $ExcelTableNm + " */`n WHERE " + $chgKey + $implbetweenarr
                 } else {
                     $deletecolarr = "DELETE /* " + $ExcelTable + "(" + $pgmLCDid + ") */`n  FROM " + ($ExcelOwner + "." + $ExcelTable).PadRight(90) + "/* " + $ExcelTableNm + " */"
                     $truncatearr = "TRUNCATE /* " + ($ExcelTable + "(" + $pgmLCDid + ") */" + $ExcelOwner + "." + $ExcelTable).PadRight(88) + "/* " + $ExcelTableNm + " */"
                     $truncatetmparr = "TRUNCATE /* " + ($ExcelTable + "(LCD_" + $ExcelTable + "_TG) */ " + $TempOwner + "." + $TempTable).PadRight(88) + "/* " + $ExcelTableNm + " */"
                     $impldeletecolarr = "DELETE /* " + $ExcelTable + "(" + $pgmLIDid + ") */`n  FROM " + ($ExcelOwner + "." + $ExcelTable).PadRight(93) + "/* " + $ExcelTableNm + " */`n WHERE " + $chgKey + $implbetweenarr
                 }
                 # TEMP 적재 INSERT 문 생성
                 # temp insert 시 변경적재 키에 해당하는 값만 insert 하도록 key = {BASE_DT} 항목 추가
                 if ($Excelchgyn -eq 'Y') {
                     $tempinsertarr = "INSERT /* " + $ExcelTable + "(" + $pgmLCDid + ") */`n  INTO " + $TempOwner + "." + $TempTable.PadRight(87) + "/* " + $ExcelTableNm.Substring(1, $ExcelTableNm.Length-1) + " */`n     (`n" + $tempselcolarr + "     )`nSELECT /* " + $ExcelTable + "(" + $pgmLCDid + ") */`n" + $tempinscolarr + "  FROM `"bludbev`".`"" + $Excelasisowner + "`".`"" + ($ExternalTable + "`"").PadRight(72) + "/* " + $Excelasistbnm + " */`n WHERE `"" + $LCDchgKey + "`"" + $betweenarr
                     $templidinsertarr = "INSERT /* " + $ExcelTable + "(" + $pgmLCDid + ") */`n  INTO " + $TempOwner + "." + $TempTable.PadRight(87) + "/* " + $ExcelTableNm.Substring(1, $ExcelTableNm.Length-1) + " */`n     (`n" + $tempselcolarr + "     )`nSELECT /* " + $ExcelTable + "(" + $pgmLCDid + ") */`n" + $tempinscolarr + "  FROM `"bludbev`".`"" + $Excelasisowner + "`".`"" + ($ExternalTable + "`"").PadRight(72) + "/* " + $Excelasistbnm + " */"
                     $implelidinsertarr = "INSERT /* " + $ExcelTable + "(" + $pgmLIDid + ") */`n  INTO " + $ExcelOwner + "." + $ExcelTable.PadRight(87) + "/* " + $ExcelTableNm.Substring(1, $ExcelTableNm.Length-1) + " */`n     (`n" + $tempselcolarr + "     )`nSELECT /* " + $ExcelTable + "(" + $pgmLIDid + ") */`n" + $tempinscolarr + "  FROM `"bludbev`".`"" + $Excelasisowner + "`".`"" + ($ExternalTable + "`"").PadRight(72) + "/* " + $Excelasistbnm + " */`n WHERE `"" + $chgKey + "`"" + $implbetweenarr
                 } else {
                     $tempinsertarr = "INSERT /* " + $ExcelTable + "(" + $pgmLCDid + ") */`n  INTO " + $TempOwner + "." + $TempTable.PadRight(87) + "/* " + $ExcelTableNm.Substring(1, $ExcelTableNm.Length-1) + " */`n     (`n" + $tempselcolarr + "     )`nSELECT /* " + $ExcelTable + "(" + $pgmLCDid + ") */`n" + $tempinscolarr + "  FROM `"bludbev`".`"" + $Excelasisowner + "`".`"" + ($ExternalTable + "`"").PadRight(72) + "/* " + $Excelasistbnm + " */"
                     $implelidinsertarr = "INSERT /* " + $ExcelTable + "(" + $pgmLIDid + ") */`n  INTO " + $ExcelOwner + "." + $ExcelTable.PadRight(87) + "/* " + $ExcelTableNm.Substring(1, $ExcelTableNm.Length-1) + " */`n     (`n" + $tempselcolarr + "     )`nSELECT /* " + $ExcelTable + "(" + $pgmLIDid + ") */`n" + $tempinscolarr + "  FROM `"bludbev`".`"" + $Excelasisowner + "`".`"" + ($ExternalTable + "`"").PadRight(72) + "/* " + $Excelasistbnm + " */`n WHERE `"" + $chgKey + "`"" + $implbetweenarr
                 }
                 # Target 적재 INSERT 문 생성
                 $tarinsertarr = "INSERT /* " + $ExcelTable + "(" + $pgmLCDid + ") */`n  INTO " + $ExcelOwner + "." + $ExcelTable.PadRight(87) + "/* " + $ExcelTableNm + " */`n     (`n" + $colarr + "     )`nSELECT /* " + $ExcelTable + "(" + $pgmLCDid + ") */`n" + $etlcolarr + "  FROM " + $TempOwner + "." + $TempTable.PadRight(87) + "/* " + $ExcelTableNm + " */"
                 $alltarinsertarr = "INSERT /* " + $ExcelTable + "(" + $pgmLCDid + ") */`n  INTO " + $ExcelOwner + "." + $ExcelTable.PadRight(87) + "/* " + $ExcelTableNm + " */`n     (`n" + $colarr + "     )`nSELECT /* " + $ExcelTable + "(" + $pgmLCDid + ") */`n" + $tempinscolarr + "  FROM `"bludbev`".`"" + $Excelasisowner + "`".`"" + ($ExternalTable + "`"").PadRight(72) + "/* " + $Excelasistbnm + " */"
              }
         }
         if($impleChk -eq "Y") {
$bodyimplesql += "/*
`####################################################################################################
`1.  작성자       : 윤혁준
`2.  타겟테이블명 : $ExcelTable ($ExcelTableNm)
`3.  작성일자     : 2023.05.23
`4.  프로시져명   : LID_$ExcelTable`_TG
`5.  모집단       :
`6.  소스테이블   : $Excelasistable ($Excelasistbnm)
`7.  특이사항     : 없음
`8.  집계시작시점 :
`9.  집계기준     : $Excelbase
`10. 변경내역     :
`####################################################################################################
`*/
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : SET                                                                            */
`/**************************************************************************************************/
`SET enable_case_sensitive_identifier TO TRUE;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Delete                                                                  */
`/**************************************************************************************************/
$impldeletecolarr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Insert                                                                  */
`/**************************************************************************************************/
$implelidinsertarr
`;
`####SQL
"
[System.IO.File]::WriteAllLines("C:\Users\Owner\Documents\source\body\초기적재\LID_$ExcelTable`_TG.sql",$bodyimplesql,$utf8NoBomEncoding)
         }
         if ($Exceljuje -eq "SAP") {
             if ($ExcelTable -eq "SFM_ZGLITEMS01") {
                 $pgmidLCD = "LCD_" + $ExcelTable + "_01"
                 $pgmidLID = "LID_" + $ExcelTable + "_01"
             } else {
                 $pgmidLCD = "LCD_" + $ExcelTable + "_TG"
                 $pgmidLID = "LID_" + $ExcelTable + "_TG"
             }
             if($Excelchgyn -eq 'Y') {

                 if($ExcelTable -eq "SFM_ZGLITEMS02") {
$bodysql += "/*
`####################################################################################################
`1.  작성자       : 윤혁준
`2.  타겟테이블명 : $ExcelTable ($ExcelTableNm)
`3.  작성일자     : 2023.05.23
`4.  프로시져명   : pgmidLCD
`5.  모집단       :
`6.  소스테이블   : $Excelasistable ($Excelasistbnm)
`7.  특이사항     : 없음
`8.  집계시작시점 :
`9.  집계기준     : $Excelbase
`10. 변경내역     :
`####################################################################################################
`*/
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : SET                                                                            */
`/**************************************************************************************************/
`SET enable_case_sensitive_identifier TO TRUE;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Delete                                                                 */
`/**************************************************************************************************/
$truncatetmparr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Copy                                                                   */
`/**************************************************************************************************/
`COPY $TempOwner.$TempTable
`FROM 's3://{S3_BUCKET}/$Excelasistable`_{BASE_DT_TO}'
`IAM_ROLE '{ARN_S3}'
`FORMAT AS CSV DELIMITER '\11'
$sapprocom
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Delete                                                                  */
`/**************************************************************************************************/
$deletecolarr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Insert                                                                  */
`/**************************************************************************************************/
$tarinsertarr
`;
`####SQL
"
## FORMAT AS CSV QUOTE '\37'(데이터에 따옴표 존재시) DELIMITER '\11'
$bodyuidsql += "/*
`####################################################################################################
`1.  작성자       : 윤혁준
`2.  타겟테이블명 : $ExcelTable ($ExcelTableNm)
`3.  작성일자     : 2023.05.23
`4.  프로시져명   : pgmidLID
`5.  모집단       :
`6.  소스테이블   : $Excelasistable ($Excelasistbnm)
`7.  특이사항     : 없음
`8.  집계시작시점 :
`9.  집계기준     : $Excelbase
`10. 변경내역     :
`####################################################################################################
`*/
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : SET                                                                            */
`/**************************************************************************************************/
`SET enable_case_sensitive_identifier TO TRUE;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Delete                                                                 */
`/**************************************************************************************************/
$truncatetmparr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Copy                                                                   */
`/**************************************************************************************************/
`COPY $TempOwner.$TempTable
`FROM 's3://{S3_BUCKET}/$Excelasistable`_{BASE_DT_TO}'
`IAM_ROLE '{ARN_S3}'
`FORMAT AS CSV DELIMITER '\11'
$sapprocom
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Delete                                                                  */
`/**************************************************************************************************/
$truncatearr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Insert                                                                  */
`/**************************************************************************************************/
$tarinsertarr
`;
`####SQL
"
$bodyupdatesql += "/*
`####################################################################################################
`1.  작성자       : 윤혁준
`2.  타겟테이블명 : SFM_ZGLITEMS01 (SFM_총계정원장_당월)
`3.  작성일자     : 2023.05.23
`4.  프로시져명   : LCD_SFM_ZGLITEMS01_TG
`5.  모집단       :
`6.  소스테이블   : $ExcelTable ($ExcelTableNm)
`7.  특이사항     : 없음
`8.  집계시작시점 :
`9.  집계기준     : $Excelbase
`10. 변경내역     :
`####################################################################################################
`*/
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : SET                                                                            */
`/**************************************************************************************************/
`SET enable_case_sensitive_identifier TO TRUE;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Update                                                                  */
`/**************************************************************************************************/
$updatearr
`;
`####SQL
"

#$bodysql > C:\Users\Owner\Documents\source\body\변경적재\SAP\LCD_$ExcelTable"_TG".sql
#$bodyuidsql > C:\Users\Owner\Documents\source\body\초기적재\SAP\LID_$ExcelTable"_TG".sql
#$bodysql | Out-File -FilePath "C:\Users\Owner\Documents\source\body\변경적재\SAP\LCD_$ExcelTable`_TG.sql" -Encoding UTF8
#$bodyuidsql | Out-File -FilePath "C:\Users\Owner\Documents\source\body\초기적재\SAP\LID_$ExcelTable`_TG.sql" -Encoding UTF8
[System.IO.File]::WriteAllLines("C:\Users\Owner\Documents\source\body\변경적재\SAP\$pgmidLCD.sql",$bodysql,$utf8NoBomEncoding)
[System.IO.File]::WriteAllLines("C:\Users\Owner\Documents\source\body\초기적재\SAP\$pgmidLID.sql",$bodyuidsql,$utf8NoBomEncoding)
[System.IO.File]::WriteAllLines("C:\Users\Owner\Documents\source\body\변경적재\SAP\LCD_SFM_ZGLITEMS01_TG.sql",$bodyupdatesql,$utf8NoBomEncoding)
                 } else { #$ExcelTable -eq "SFM_ZGLITEMS02"

$bodysql += "/*
`####################################################################################################
`1.  작성자       : 윤혁준
`2.  타겟테이블명 : $ExcelTable ($ExcelTableNm)
`3.  작성일자     : 2023.05.23
`4.  프로시져명   : $pgmidLCD
`5.  모집단       :
`6.  소스테이블   : $Excelasistable ($Excelasistbnm)
`7.  특이사항     : 없음
`8.  집계시작시점 :
`9.  집계기준     : $Excelbase
`10. 변경내역     :
`####################################################################################################
`*/
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : SET                                                                            */
`/**************************************************************************************************/
`SET enable_case_sensitive_identifier TO TRUE;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Delete                                                                 */
`/**************************************************************************************************/
$truncatetmparr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Copy                                                                   */
`/**************************************************************************************************/
`COPY $TempOwner.$TempTable
`FROM 's3://{S3_BUCKET}/$Excelasistable`_{BASE_DT_TO}'
`IAM_ROLE '{ARN_S3}'
`FORMAT AS CSV DELIMITER '\11'
$sapprocom
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Delete                                                                  */
`/**************************************************************************************************/
$deletecolarr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Insert                                                                  */
`/**************************************************************************************************/
$tarinsertarr
`;
`####SQL
"
$bodyuidsql += "/*
`####################################################################################################
`1.  작성자       : 윤혁준
`2.  타겟테이블명 : $ExcelTable ($ExcelTableNm)
`3.  작성일자     : 2023.05.23
`4.  프로시져명   : $pgmidLID
`5.  모집단       :
`6.  소스테이블   : $Excelasistable ($Excelasistbnm)
`7.  특이사항     : 없음
`8.  집계시작시점 :
`9.  집계기준     : $Excelbase
`10. 변경내역     :
`####################################################################################################
`*/
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : SET                                                                            */
`/**************************************************************************************************/
`SET enable_case_sensitive_identifier TO TRUE;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Delete                                                                 */
`/**************************************************************************************************/
$truncatetmparr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Copy                                                                   */
`/**************************************************************************************************/
`COPY $TempOwner.$TempTable
`FROM 's3://{S3_BUCKET}/$Excelasistable`_{BASE_DT_TO}'
`IAM_ROLE '{ARN_S3}'
`FORMAT AS CSV DELIMITER '\11'
$sapprocom
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Delete                                                                  */
`/**************************************************************************************************/
$truncatearr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Insert                                                                  */
`/**************************************************************************************************/
$tarinsertarr
`;
`####SQL
"

#$bodysql > C:\Users\Owner\Documents\source\body\변경적재\SAP\LCD_$ExcelTable"_TG".sql
#$bodyuidsql > C:\Users\Owner\Documents\source\body\초기적재\SAP\LID_$ExcelTable"_TG".sql
#$bodysql | Out-File -FilePath "C:\Users\Owner\Documents\source\body\변경적재\SAP\LCD_$ExcelTable`_TG.sql" -Encoding UTF8
#$bodyuidsql | Out-File -FilePath "C:\Users\Owner\Documents\source\body\초기적재\SAP\LID_$ExcelTable`_TG.sql" -Encoding UTF8
[System.IO.File]::WriteAllLines("C:\Users\Owner\Documents\source\body\변경적재\SAP\$pgmidLCD.sql",$bodysql,$utf8NoBomEncoding)
[System.IO.File]::WriteAllLines("C:\Users\Owner\Documents\source\body\초기적재\SAP\$pgmidLID.sql",$bodyuidsql,$utf8NoBomEncoding)
                 } # else $ExcelTable -eq "SFM_ZGLITEMS02"
             } else { #$Excelchgyn -eq 'Y'
$bodysql += "/*
`####################################################################################################
`1.  작성자       : 윤혁준
`2.  타겟테이블명 : $ExcelTable ($ExcelTableNm)
`3.  작성일자     : 2023.05.23
`4.  프로시져명   : LCD_$ExcelTable`_TG
`5.  모집단       :
`6.  소스테이블   : $Excelasistable ($Excelasistbnm)
`7.  특이사항     : 없음
`8.  집계시작시점 :
`9.  집계기준     : $Excelbase
`10. 변경내역     :
`####################################################################################################
`*/
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : SET                                                                            */
`/**************************************************************************************************/
`SET enable_case_sensitive_identifier TO TRUE;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Delete                                                                 */
`/**************************************************************************************************/
$truncatetmparr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Copy                                                                   */
`/**************************************************************************************************/
`COPY $TempOwner.$TempTable
`FROM 's3://{S3_BUCKET}/$Excelasistable`_{BASE_DT_TO}'
`IAM_ROLE '{ARN_S3}'
`FORMAT AS CSV DELIMITER '\11'
$sapprocom
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Delete                                                                  */
`/**************************************************************************************************/
$truncatearr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Insert                                                                  */
`/**************************************************************************************************/
$tarinsertarr
`;
`####SQL
"

#$bodysql > C:\Users\Owner\Documents\source\body\변경적재\SAP\LCD_$ExcelTable"_TG".sql
#$bodysql | Out-File -FilePath "C:\Users\Owner\Documents\source\body\변경적재\SAP\LCD_$ExcelTable`_TG.sql" -Encoding UTF8
[System.IO.File]::WriteAllLines("C:\Users\Owner\Documents\source\body\변경적재\SAP\$pgmidLCD.sql",$bodysql,$utf8NoBomEncoding)
             } # else $Excelchgyn -eq 'Y'
         } elseif ($Exceljuje -eq "SCRM") {
             if($Excelchgyn -eq 'Y') {
$bodysql += "/*
`####################################################################################################
`1.  작성자       : 윤혁준
`2.  타겟테이블명 : $ExcelTable ($ExcelTableNm)
`3.  작성일자     : 2023.05.23
`4.  프로시져명   : $pgmidLCD
`5.  모집단       :
`6.  소스테이블   : $Excelasistable ($Excelasistbnm)
`7.  특이사항     : 없음
`8.  집계시작시점 :
`9.  집계기준     : $Excelbase
`10. 변경내역     :
`####################################################################################################
`*/
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : SET                                                                            */
`/**************************************************************************************************/
`SET enable_case_sensitive_identifier TO TRUE;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Delete                                                                 */
`/**************************************************************************************************/
$truncatetmparr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Copy                                                                   */
`/**************************************************************************************************/
`COPY $TempOwner.$TempTable
`FROM 's3://{S3_BUCKET}/$Excelasistable`_{BASE_DT_TO}'
`IAM_ROLE '{ARN_S3}'
`DELIMITER '|'
$scrmprocom
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Delete                                                                  */
`/**************************************************************************************************/
$deletecolarr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Insert                                                                  */
`/**************************************************************************************************/
$tarinsertarr
`;
`####SQL
"
$bodyuidsql += "/*
`####################################################################################################
`1.  작성자       : 윤혁준
`2.  타겟테이블명 : $ExcelTable ($ExcelTableNm)
`3.  작성일자     : 2023.05.23
`4.  프로시져명   : $pgmidLID
`5.  모집단       :
`6.  소스테이블   : $Excelasistable ($Excelasistbnm)
`7.  특이사항     : 없음
`8.  집계시작시점 :
`9.  집계기준     : $Excelbase
`10. 변경내역     :
`####################################################################################################
`*/
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : SET                                                                            */
`/**************************************************************************************************/
`SET enable_case_sensitive_identifier TO TRUE;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Delete                                                                 */
`/**************************************************************************************************/
$truncatetmparr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Copy                                                                   */
`/**************************************************************************************************/
`COPY $TempOwner.$TempTable
`FROM 's3://{S3_BUCKET}/$Excelasistable`_{BASE_DT_TO}'
`IAM_ROLE '{ARN_S3}'
`DELIMITER '|'
$scrmprocom
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Delete                                                                  */
`/**************************************************************************************************/
$truncatearr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Insert                                                                  */
`/**************************************************************************************************/
$tarinsertarr
`;
`####SQL
"

#$bodysql > C:\Users\Owner\Documents\source\body\변경적재\SCRM\LCD_$ExcelTable"_TG".sql
#$bodyuidsql > C:\Users\Owner\Documents\source\body\초기적재\SCRM\LID_$ExcelTable"_TG".sql
#$bodysql | Out-File -FilePath "C:\Users\Owner\Documents\source\body\변경적재\SCRM\LCD_$ExcelTable`_TG.sql" -Encoding UTF8
#$bodyuidsql | Out-File -FilePath "C:\Users\Owner\Documents\source\body\초기적재\SCRM\LID_$ExcelTable`_TG.sql" -Encoding UTF8
[System.IO.File]::WriteAllLines("C:\Users\Owner\Documents\source\body\변경적재\SCRM\LCD_$ExcelTable`_TG.sql",$bodysql,$utf8NoBomEncoding)
[System.IO.File]::WriteAllLines("C:\Users\Owner\Documents\source\body\초기적재\SCRM\LID_$ExcelTable`_TG.sql",$bodyuidsql,$utf8NoBomEncoding)
             } else {
$bodysql += "/*
`####################################################################################################
`1.  작성자       : 윤혁준
`2.  타겟테이블명 : $ExcelTable ($ExcelTableNm)
`3.  작성일자     : 2023.05.23
`4.  프로시져명   : LCD_$ExcelTable`_TG
`5.  모집단       :
`6.  소스테이블   : $Excelasistable ($Excelasistbnm)
`7.  특이사항     : 없음
`8.  집계시작시점 :
`9.  집계기준     : $Excelbase
`10. 변경내역     :
`####################################################################################################
`*/
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : SET                                                                            */
`/**************************************************************************************************/
`SET enable_case_sensitive_identifier TO TRUE;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Delete                                                                 */
`/**************************************************************************************************/
$truncatetmparr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Copy                                                                   */
`/**************************************************************************************************/
`COPY $TempOwner.$TempTable
`FROM 's3://{S3_BUCKET}/$Excelasistable`_{BASE_DT_TO}'
`IAM_ROLE '{ARN_S3}'
`DELIMITER '|'
$scrmprocom
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Delete                                                                  */
`/**************************************************************************************************/
$truncatearr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Insert                                                                  */
`/**************************************************************************************************/
$tarinsertarr
`;
`####SQL
"

#$bodysql > C:\Users\Owner\Documents\source\body\변경적재\SCRM\LCD_$ExcelTable"_TG".sql
#$bodysql | Out-File -FilePath "C:\Users\Owner\Documents\source\body\변경적재\SCRM\LCD_$ExcelTable`_TG.sql" -Encoding UTF8
[System.IO.File]::WriteAllLines("C:\Users\Owner\Documents\source\body\변경적재\SCRM\LCD_$ExcelTable`_TG.sql",$bodysql,$utf8NoBomEncoding)
             }
         } else { # elseif $Exceljuje -eq "SCRM"
             if($Excelchgyn -eq 'Y') {
$bodysql += "/*
`####################################################################################################
`1.  작성자       : 윤혁준
`2.  타겟테이블명 : $ExcelTable ($ExcelTableNm)
`3.  작성일자     : 2023.05.23
`4.  프로시져명   : LCD_$ExcelTable`_TG
`5.  모집단       :
`6.  소스테이블   : $Excelasistable ($Excelasistbnm)
`7.  특이사항     : 없음
`8.  집계시작시점 :
`9.  집계기준     : $Excelbase
`10. 변경내역     :
`####################################################################################################
`*/
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : SET                                                                            */
`/**************************************************************************************************/
`SET enable_case_sensitive_identifier TO TRUE;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Delete                                                                 */
`/**************************************************************************************************/
$truncatetmparr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Insert                                                                 */
`/**************************************************************************************************/
$tempinsertarr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Delete                                                                  */
`/**************************************************************************************************/
$deletecolarr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Insert                                                                  */
`/**************************************************************************************************/
$tarinsertarr
`;
`####SQL
"
<#$bodyuidsql += "/*
`####################################################################################################
`1.  작성자       : 윤혁준
`2.  타겟테이블명 : $ExcelTable ($ExcelTableNm)
`3.  작성일자     : 2023.05.23
`4.  프로시져명   : LID_$ExcelTable`_TG
`5.  모집단       :
`6.  소스테이블   : $Excelasistable ($Excelasistbnm)
`7.  특이사항     : 없음
`8.  집계시작시점 :
`9.  집계기준     : $Excelbase
`10. 변경내역     :
`####################################################################################################
`*/
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : SET                                                                            */
`/**************************************************************************************************/
`SET enable_case_sensitive_identifier TO TRUE;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Delete                                                                 */
`/**************************************************************************************************/
$truncatetmparr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Insert                                                                 */
`/**************************************************************************************************/
$templidinsertarr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Delete                                                                  */
`/**************************************************************************************************/
$truncatearr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Insert                                                                  */
`/**************************************************************************************************/
$tarinsertarr
`;
`####SQL
"#>
#$bodysql > C:\Users\Owner\Documents\source\body\변경적재\LCD_$ExcelTable"_TG".sql
#$bodyuidsql > C:\Users\Owner\Documents\source\body\초기적재\LID_$ExcelTable"_TG".sql
#$bodysql | Out-File -FilePath "C:\Users\Owner\Documents\source\body\변경적재\LCD_$ExcelTable`_TG.sql" -Encoding UTF8
#$bodyuidsql | Out-File -FilePath "C:\Users\Owner\Documents\source\body\초기적재\LID_$ExcelTable`_TG.sql" -Encoding UTF8
[System.IO.File]::WriteAllLines("C:\Users\Owner\Documents\source\body\변경적재\LCD_$ExcelTable`_TG.sql",$bodysql,$utf8NoBomEncoding)
#[System.IO.File]::WriteAllLines("C:\Users\Owner\Documents\source\body\초기적재\LID_$ExcelTable`_TG.sql",$bodyuidsql,$utf8NoBomEncoding)
             } elseif ($Excelchgyn -eq 'S') {
$bodysql += "/*
`####################################################################################################
`1.  작성자       : 윤혁준
`2.  타겟테이블명 : $ExcelTable ($ExcelTableNm)
`3.  작성일자     : 2023.05.23
`4.  프로시져명   : LCD_$ExcelTable`_TG
`5.  모집단       :
`6.  소스테이블   : $Excelasistable ($Excelasistbnm)
`7.  특이사항     : 없음
`8.  집계시작시점 :
`9.  집계기준     : $Excelbase
`10. 변경내역     :
`####################################################################################################
`*/
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : SET                                                                            */
`/**************************************************************************************************/
`SET enable_case_sensitive_identifier TO TRUE;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : SNAPSHOT Delete                                                                */
`/**************************************************************************************************/
$snpdeletecolarr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : SNAPSHOT Insert                                                                */
`/**************************************************************************************************/
$snptarinsertarr
`;
`####SQL
"
#$bodysql > C:\Users\Owner\Documents\source\body\변경적재\LCD_$ExcelTable"_TG".sql
#$bodysql | Out-File -FilePath "C:\Users\Owner\Documents\source\body\변경적재\LCD_$ExcelTable`_TG.sql" -Encoding UTF8
#$bodyuidsql | Out-File -FilePath "C:\Users\Owner\Documents\source\body\초기적재\SAP\LID_$ExcelTable`_TG.sql" -Encoding UTF8
[System.IO.File]::WriteAllLines("C:\Users\Owner\Documents\source\body\변경적재\LCD_$ExcelTable`_TG.sql",$bodysql,$utf8NoBomEncoding)
             } else {
$bodysql += "/*
`####################################################################################################
`1.  작성자       : 윤혁준
`2.  타겟테이블명 : $ExcelTable ($ExcelTableNm)
`3.  작성일자     : 2023.05.23
`4.  프로시져명   : LCD_$ExcelTable`_TG
`5.  모집단       :
`6.  소스테이블   : $Excelasistable ($Excelasistbnm)
`7.  특이사항     : 없음
`8.  집계시작시점 :
`9.  집계기준     : $Excelbase
`10. 변경내역     :
`####################################################################################################
`*/
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : SET                                                                            */
`/**************************************************************************************************/
`SET enable_case_sensitive_identifier TO TRUE;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Delete                                                                  */
`/**************************************************************************************************/
$truncatearr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Target Insert                                                                  */
`/**************************************************************************************************/
$alltarinsertarr
`;
`####SQL
"
#$bodysql > C:\Users\Owner\Documents\source\body\변경적재\LCD_$ExcelTable"_TG".sql
#$bodysql | Out-File -FilePath "C:\Users\Owner\Documents\source\body\변경적재\LCD_$ExcelTable`_TG.sql" -Encoding UTF8
[System.IO.File]::WriteAllLines("C:\Users\Owner\Documents\source\body\변경적재\LCD_$ExcelTable`_TG.sql",$bodysql,$utf8NoBomEncoding)
             }
         }
     }
<#`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Delete                                                                 */
`/**************************************************************************************************/
$truncatetmparr
`;
`####SQL
`/**************************************************************************************************/
`/*  P_BAT_STG_NM : Staging Insert                                                                 */
`/**************************************************************************************************/
$tempinsertarr
`;
`####SQL
#> ### 2024.01.04 전체적재 테이블의 경우 staging을 거치지않고 Target 바로적재 (소스 원복의 경우를 생각하여 소스 옮겨둠)
     $ExcelWorkBook.Close()
     #$connection.close()
     $redconnection.close()
     $endTime = (Get-Date)
     $timeSpan = $endTime  - $startTime
     Write-Host "======================================="
     Write-Host "======>>작업완료"
     Write-Host "======>>$($timeSpan.TotalSeconds) seconds to run"
     Write-Host "======================================="

} catch {
     Write-Error ("Database Exception:{0}{1}" -f ` $con.ConnectionString, $_.Exception.ToString())
} finally{
     if ($redconnection.State -eq 'Open') {
        $redconnection.close()
     }
}