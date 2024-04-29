Add-Type -AssemblyName System.Data.OracleClient

$PSDefaultParameterValues['*:Encoding'] = 'Default'

# 운영계 개발
#$driver = "MySQL ODBC 8.0 ANSI Driver"
#$server = "dept-rds-an2-op-dev-opmdsdbd.cluster-c92dc3uwdt0o.ap-northeast-2.rds.amazonaws.com"
#$username = "FEDSEL"
#$password = "d55!43EL"
#$data_source = "MDSDEV"
#$connection_string = "User Id=$username;Password=$password;Data Source=$data_source"
#$connectionString = "DRIVER={$driver};Server=$server;Port=3306;Database=$data_source;Uid=$username;Pwd=$password"

# 운영계 STG
$driver = "MySQL ODBC 8.0 ANSI Driver"
$server = "dept-rds-an2-op-stg-opmdsdbt.cluster-ro-ckcic5dtfarb.ap-northeast-2.rds.amazonaws.com"
$username = "ANFED"
$password = "d55!43ED"
$data_source = "MDSSTG"
#$connection_string = "User Id=$username;Password=$password;Data Source=$data_source"
$connectionString = "DRIVER={$driver};Server=$server;Port=3306;Database=$data_source;Uid=$username;Pwd=$password"

# 분석계 개발DB(Redshift)
$redhost = "dept-rsc-an2-cm-dwdev-ansla.ctdnukmkv08j.ap-northeast-2.redshift.amazonaws.com"
$redport = 5439
$reddbName = "bludbev"
$redusername = "etluser08"
$redpassword = "!Etluser08"
$redconnectionString = "Driver={Amazon Redshift (x64)}; Server=$redhost; Port=$redport; Database=$reddbName; UID=$redusername; PWD=$redpassword;"

try {
     #작업 목록
     $ExcelObj = new-Object -Comobject Excel.Application
     $ExcelObj.visible=$false

     $ExcelWorkBook = $ExcelObj.Workbooks.Open("C:\Users\Owner\Desktop\신세계백화점 차세대 시스템 구축\03. 개발\99. 관리문서\SSGDX_A_AN_TOBE-SOR 테이블 목록.xlsx")
     $ExcelWorkSheet = $ExcelWorkBook.Sheets.Item("SOR 목록")
     
     $rowcount=$ExcelWorkSheet.UsedRange.Rows.Count

     $Path = "C:\Users\Owner\Documents\source\레이아웃비교\비교.xlsx"
     # Excel 개체 생성
     $excel = New-Object -ComObject Excel.Application
     # 새 문서 생성
     $workbook = $excel.Workbooks.Add()
     # Excel 창 표시
     $excel.Visible = $false
     $worksheet = $workbook.Worksheets.Item(1)

     #제외 대상 컬럼 목록
     $ExcelObj1 = new-Object -Comobject Excel.Application
     $ExcelObj1.visible=$false

     $ExcelWorkBook1 = $ExcelObj1.Workbooks.Open("C:\Users\Owner\Desktop\신세계백화점 차세대 시스템 구축\03. 개발\99. 관리문서\운영계_컬럼보안항목.xlsx")
     $ExcelWorkSheet1 = $ExcelWorkBook1.Sheets.Item("ATTRIBUTE LIST")
     
     $rowcount1=$ExcelWorkSheet1.UsedRange.Rows.Count

     $tarA = 2
     $sorA = 2

     $worksheet.Cells.Item(1, 2).Value2 = "Target Owner"
     $worksheet.Cells.Item(1, 3).Value2 = "Target Table"
     $worksheet.Cells.Item(1, 4).Value2 = "Target TableNM"
     $worksheet.Cells.Item(1, 5).Value2 = "Target Column"
     $worksheet.Cells.Item(1, 6).Value2 = "Target ColumnNM"
     $worksheet.Cells.Item(1, 7).Value2 = "Target Data Type"
     $worksheet.Cells.Item(1, 8).Value2 = "Target Length"
     $worksheet.Cells.Item(1, 9).Value2 = "Target Precision"
     $worksheet.Cells.Item(1, 10).Value2 = "Target Scale"
     $worksheet.Cells.Item(1, 11).Value2 = "Target Nullable"
     $worksheet.Cells.Item(1, 12).Value2 = "Source Owner"
     $worksheet.Cells.Item(1, 13).Value2 = "Source Table"
     $worksheet.Cells.Item(1, 14).Value2 = "Source TableNM"
     $worksheet.Cells.Item(1, 15).Value2 = "Source Column"
     $worksheet.Cells.Item(1, 16).Value2 = "Source ColumnNM"
     $worksheet.Cells.Item(1, 17).Value2 = "Source Data Type"
     $worksheet.Cells.Item(1, 18).Value2 = "Source Length"
     $worksheet.Cells.Item(1, 19).Value2 = "Source Precision"
     $worksheet.Cells.Item(1, 20).Value2 = "Source Scale"
     $worksheet.Cells.Item(1, 21).Value2 = "Source Nullable"

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

         if ($Excelasistable -eq "X") {
             continue
         }

         if ($ExcelTableNm.Contains("스냅샷")) {
             continue
         }

         $Excelasisowner = "MDSDEV"
         Write-Host "--------------------" $ExcelOwner"."$ExcelTable
         $ExcelredOwner = $ExcelOwner.ToLower()
         $Excelredtable = $ExcelTable.ToLower()
         $redstatement = "SELECT T10.TABLE_SCHEMA
`                               , T10.TABLE_NAME
`                               , COALESCE(obj_description('" + $ExcelredOwner + "." + $Excelredtable + "'::regclass), '') AS table_comment
`                               , T20.COLUMN_NAME
`                               , COALESCE(T40.description::varchar, '')
`                               , T20.ORDINAL_POSITION
`                               , COALESCE(CASE WHEN T20.DATA_TYPE = 'character varying' or T20.DATA_TYPE = 'character'
`                                      THEN 'VARCHAR(' || T20.CHARACTER_MAXIMUM_LENGTH || ')'
`                                      WHEN T20.DATA_TYPE = 'smallint'
`                                      THEN 'INTEGER(' || T20.NUMERIC_PRECISION || ')'
`                                      ELSE T20.DATA_TYPE
`                                  END,'없음')
`                               , T20.IS_NULLABLE
`                               , COALESCE(T20.COLUMN_DEFAULT,'')
`                               , T20.DATA_TYPE
`                               , COALESCE(T20.NUMERIC_PRECISION,0)
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
`                           WHERE T10.TABLE_SCHEMA = '" + $ExcelredOwner + "'
`                             AND T10.TABLE_NAME = '" + $Excelredtable + "'
`                           ORDER BY T10.TABLE_NAME, T20.ORDINAL_POSITION
`;"
         # 타겟 레이아웃을 불러오기 위한 분석계 개발DB 연결
         $redconnection = New-Object System.Data.Odbc.OdbcConnection($redconnectionString)
         $redconnection.Open()

         $redcommand = New-Object System.Data.Odbc.OdbcCommand($redstatement, $redconnection)
         $redcommand.CommandText = $redstatement
         $redresult = $redcommand.ExecuteReader()

         #$redresultcnt = $redresult.RecordsAffected
         $redresultcnt = 0

         while ($redresult.Read())
         {
          $sorOwner = $redresult.GetString(0)
          $sortableid = $redresult.GetString(1)
          $sortablenm = $redresult.GetString(2)
          $sorcolumnid = $redresult.GetString(3)
          $sorcolumnnm = $redresult.GetString(4)
          $sorposition = $redresult.GetValue(5)
          $sorcolumntype = $redresult.GetString(6)
          $sornullable = $redresult.GetString(7)
          $sordefault = $redresult.GetString(8)
          $sordatatype = $redresult.GetString(9)
          $sorprecision = $redresult.GetValue(10)
          $sorscale = $redresult.GetValue(11)
          $sormaxlength = $redresult.GetValue(12)

          if ($sorcolumnid -eq "etl_wrk_dtm") {
              continue
          }

          if ($sornullable -eq "NO") {
              $sornullable = "NOT NULL"
          } else {
              $sornullable = "NULL"
          }

          if ($sordatatype -eq "character varying") {
              $sordatatype = "VARCHAR"
          } elseif ($sordatatype -eq "timestamp without time zone") {
              $sordatatype = "TIMESTAMP"
          } elseif ($sordatatype -eq "character") {
              $sordatatype = "CHAR"
          } else {
              $sordatatype = $sordatatype
          }
          $worksheet.Cells.Item($tarA, 2).Value2 = $sorOwner
          $worksheet.Cells.Item($tarA, 3).Value2 = $sortableid.ToUpper()
          $worksheet.Cells.Item($tarA, 4).Value2 = $sortablenm
          $worksheet.Cells.Item($tarA, 5).Value2 = $sorcolumnid.ToUpper()
          $worksheet.Cells.Item($tarA, 6).Value2 = $sorcolumnnm
          $worksheet.Cells.Item($tarA, 7).Value2 = $sordatatype.ToUpper()
          $worksheet.Cells.Item($tarA, 8).Value2 = $sormaxlength.ToString()
          $worksheet.Cells.Item($tarA, 9).Value2 = $sorprecision.ToString()
          $worksheet.Cells.Item($tarA, 10).Value2 = $sorscale.ToString()
          $worksheet.Cells.Item($tarA, 11).Value2 = $sornullable

          $tarA = $tarA + 1
          $redresultcnt = $redresultcnt + 1
         } # 타겟 While 끝

         $statement = "SELECT A.TABLE_SCHEMA
`                           , A.TABLE_NAME
`                           , A.TABLE_COMMENT
`                           , B.COLUMN_NAME
`                           , B.COLUMN_COMMENT
`                           , B.ORDINAL_POSITION
`                           , B.COLUMN_TYPE
`                           , B.COLUMN_KEY
`                           , B.IS_NULLABLE
`                           , COALESCE(B.COLUMN_DEFAULT,'')
`                           , B.DATA_TYPE
`                           , COALESCE(B.NUMERIC_PRECISION,0)
`                           , COALESCE(B.NUMERIC_SCALE,0)
`                           , COALESCE(B.CHARACTER_MAXIMUM_LENGTH,0)
`                        FROM INFORMATION_SCHEMA.`TABLES` A
`                        LEFT
`                        JOIN INFORMATION_SCHEMA.`COLUMNS` B
`                          ON A.TABLE_SCHEMA = B.TABLE_SCHEMA
`     					  AND A.TABLE_NAME = B.TABLE_NAME
`                       WHERE A.TABLE_SCHEMA = '" + $Excelasisowner + "'
`                         AND A.TABLE_NAME = '" + $Excelasistable + "'
`                       ORDER BY A.TABLE_NAME, B.ORDINAL_POSITION
;"
         # 운영계 레이아웃을 불러오기 위한 AuroraDB 연결
         $connection = New-Object System.Data.Odbc.OdbcConnection($connectionString)
         $connection.Open()

         $command = New-Object System.Data.Odbc.OdbcCommand($statement, $connection)
         $command.CommandText = $statement
         $result = $command.ExecuteReader()
         
         #$resultcnt = $result.RecordsAffected
         $resultcnt = 0

         while ($result.Read())
         {
          $Owner = $result.GetString(0)
          $tableid = $result.GetString(1)
          $tablename = $result.GetString(2)
          $columnid = $result.GetString(3)
          $columnname = $result.GetString(4)
          $position = $result.GetValue(5)
          $columntype = $result.GetString(6)
          $pkyn = $result.GetString(7)
          $nullable = $result.GetString(8)
          $default = $result.GetString(9)
          $datatype = $result.GetString(10)
          $precision = $result.GetValue(11)
          $scale = $result.GetValue(12)
          $maxlength = $result.GetValue(13)

          if ($columnid -eq "BRNO" -or $columnid -eq "PIC_MP_NTNO" -or $columnid -eq "PIC_MP_TEXNO" -or $columnid -eq "PIC_MP_INDNO" -or $columnid -eq "STR_ADDR" -or $columnid -eq "TOBJ_ADDR" -or $columnid -eq "BRNO" -or $columnid -eq "CRNO" -or $columnid -eq "BZPL_ZIP" -or $columnid -eq "BZPL_ADDR" -or $columnid -eq "BZPL_DTLAD" -or $columnid -eq "HDSTR_ZIP" -or $columnid -eq "HDSTR_ADDR" -or $columnid -eq "HDSTR_DTLAD" -or $columnid -eq "CTR_TGTR_RRNO" -or $columnid -eq "CTR_TGTR_MP_TEXNO" -or $columnid -eq "CTR_TGTR_MP_NTNO" -or $columnid -eq "CTR_TGTR_MP_INDNO" -or $columnid -eq "BRNO" -or $columnid -eq "CRNO" -or $columnid -eq "BZPL_ZIP" -or $columnid -eq "BZPL_ADDR" -or $columnid -eq "BZPL_DTLAD" -or $columnid -eq "HDSTR_ZIP" -or $columnid -eq "HDSTR_ADDR" -or $columnid -eq "HDSTR_DTLAD" -or $columnid -eq "REQR_TELNO" -or $columnid -eq "BRNO" -or $columnid -eq "BRNO" -or $columnid -eq "BRNO" -or $columnid -eq "APLR_TEL_TEXNO" -or $columnid -eq "APLR_TEL_ARNO" -or $columnid -eq "APLR_TEL_INDNO" -or $columnid -eq "APLR_MP_TEXNO" -or $columnid -eq "APLR_MP_NTNO" -or $columnid -eq "APLR_MP_INDNO" -or $columnid -eq "APLR_EML_ADDR" -or $columnid -eq "PWD" -or $columnid -eq "PIC_MP_TEXNO" -or $columnid -eq "PIC_MP_NTNO" -or
              $columnid -eq "PIC_MP_INDNO" -or $columnid -eq "CETI_PWD" -or $columnid -eq "BRNO" -or $columnid -eq "CRNO" -or $columnid -eq "BZPL_ZIP" -or $columnid -eq "BZPL_ADDR" -or $columnid -eq "BZPL_DTLAD" -or $columnid -eq "HDSTR_ZIP" -or $columnid -eq "HDSTR_ADDR" -or $columnid -eq "HDSTR_DTLAD" -or $columnid -eq "CTR_TGTR_RRNO" -or $columnid -eq "MP_TEXNO" -or $columnid -eq "MP_NTNO" -or $columnid -eq "MP_INDNO" -or $columnid -eq "BRNO" -or $columnid -eq "RCIVR_ACTNO" -or $columnid -eq "CPCO_ADDR" -or $columnid -eq "BRNO" -or $columnid -eq "TOBJ_ADDR" -or $columnid -eq "CRNO" -or $columnid -eq "BON_COPE_MEMO" -or $columnid -eq "BRNO" -or $columnid -eq "ACTNO" -or $columnid -eq "PAY_RLNM_CRFN_NO" -or $columnid -eq "BRNO" -or $columnid -eq "BRNO" -or $columnid -eq "ACTNO" -or $columnid -eq "PAY_RLNM_CRFN_NO" -or $columnid -eq "BRNO" -or $columnid -eq "SSG_CORP_ADDR" -or $columnid -eq "SSG_CORP_DTLAD" -or $columnid -eq "ACTNO" -or $columnid -eq "RCVR_TELNO" -or $columnid -eq "BRNO" -or $columnid -eq "SSG_CORP_ADDR" -or $columnid -eq "SSG_CORP_DTLAD" -or $columnid -eq "ACTNO" -or $columnid -eq "RCVR_TELNO" -or $columnid -eq "BRNO" -or $columnid -eq "HO_BRNO" -or $columnid -eq "SHOP_TELNO" -or $columnid -eq "SHOP_ADDR" -or $columnid -eq "SHOP_DTLAD" -or
              $columnid -eq "SHOP_BRNO" -or $columnid -eq "SHOP_BZMN_WHL_ADDR" -or $columnid -eq "BRD_HO_TEL_ARNO" -or $columnid -eq "BRD_HO_TEL_TEXNO" -or $columnid -eq "BRD_HO_TEL_INDNO" -or $columnid -eq "SHOP_ADDR" -or $columnid -eq "USR_IP_ADDR" -or $columnid -eq "CRNO" -or $columnid -eq "CORP_ZIP" -or $columnid -eq "CORP_ADDR" -or $columnid -eq "CORP_DTLAD" -or $columnid -eq "CORP_TEL_ARNO" -or $columnid -eq "CORP_TEL_TEXNO" -or $columnid -eq "CORP_TEL_INDNO" -or $columnid -eq "FAX_ARNO" -or $columnid -eq "FAX_TEXNO" -or $columnid -eq "FAX_INDNO" -or $columnid -eq "BRNO" -or $columnid -eq "STR_ZIP" -or $columnid -eq "STR_ADDR" -or $columnid -eq "STR_DTLAD" -or $columnid -eq "STR_REPS_TEL_ARNO" -or $columnid -eq "STR_REPS_TEL_TEXNO" -or $columnid -eq "STR_REPS_TEL_INDNO" -or $columnid -eq "CST_CETR_TEL_ARNO" -or $columnid -eq "CST_CETR_TEL_TEXNO" -or $columnid -eq "CST_CETR_TEL_INDNO" -or $columnid -eq "BEFO_BRNO" -or $columnid -eq "RCP_NMPL_BRNO" -or $columnid -eq "DPS_ACTNO" -or $columnid -eq "PAY_RLNM_CRFN_NO" -or $columnid -eq "DPS_ACTNO" -or $columnid -eq "PAY_RLNM_CRFN_NO" -or $columnid -eq "DPS_ACTNO" -or $columnid -eq "PAY_RLNM_CRFN_NO" -or $columnid -eq "DPS_ACTNO" -or $columnid -eq "PAY_RLNM_CRFN_NO" -or $columnid -eq "OVRS_CPCO_ZIP" -or
              $columnid -eq "OVRS_CPCO_ADDR" -or $columnid -eq "OVRS_CPCO_DTLAD" -or $columnid -eq "OVRS_CPCO_TELNO" -or $columnid -eq "OVRS_CPCO_EML_ADDR" -or $columnid -eq "CRNO" -or $columnid -eq "BRNO" -or $columnid -eq "CPCO_RRNO" -or $columnid -eq "CPCO_ZIP" -or $columnid -eq "CPCO_ADDR" -or $columnid -eq "CPCO_DTLAD" -or $columnid -eq "CPCO_TEL_ARNO" -or $columnid -eq "CPCO_TEL_TEXNO" -or $columnid -eq "CPCO_TEL_INDNO" -or $columnid -eq "CPCO_TEL_EXGNO" -or $columnid -eq "CPCO_FAX_ARNO" -or $columnid -eq "CPCO_FAX_TEXNO" -or $columnid -eq "CPCO_FAX_INDNO" -or $columnid -eq "CPCO_PIC_TEL_ARNO" -or $columnid -eq "CPCO_PIC_TEL_TEXNO" -or $columnid -eq "CPCO_PIC_TEL_INDNO" -or $columnid -eq "CPCO_EML_ADDR" -or $columnid -eq "HDSTR_ZIP" -or $columnid -eq "HDSTR_ADDR" -or $columnid -eq "HDSTR_DTLAD" -or $columnid -eq "CRNO" -or $columnid -eq "BRNO" -or $columnid -eq "CPCO_RRNO" -or $columnid -eq "CPCO_ZIP" -or $columnid -eq "CPCO_ADDR" -or $columnid -eq "CPCO_DTLAD" -or $columnid -eq "CPCO_TEL_ARNO" -or $columnid -eq "CPCO_TEL_TEXNO" -or $columnid -eq "CPCO_TEL_INDNO" -or $columnid -eq "CPCO_TEL_EXGNO" -or $columnid -eq "CPCO_FAX_ARNO" -or $columnid -eq "CPCO_FAX_TEXNO" -or $columnid -eq "CPCO_FAX_INDNO" -or $columnid -eq "CPCO_PIC_TEL_ARNO" -or $columnid -eq "CPCO_PIC_TEL_TEXNO" -or
              $columnid -eq "CPCO_PIC_TEL_INDNO" -or $columnid -eq "CPCO_EML_ADDR" -or $columnid -eq "HDSTR_ZIP" -or $columnid -eq "HDSTR_ADDR" -or $columnid -eq "HDSTR_DTLAD" -or $columnid -eq "CRD_CADNO" -or $columnid -eq "CRD_CADNO" -or $columnid -eq "CRD_CADNO" -or $columnid -eq "CRD_CADNO" -or $columnid -eq "BRNO" -or $columnid -eq "CRD_CADNO" -or $columnid -eq "BRNO" -or $columnid -eq "PCHR_TEL_ARNO" -or $columnid -eq "PCHR_TEL_TEXNO" -or $columnid -eq "PCHR_TEL_INDNO" -or $columnid -eq "CST_ADDR" -or $columnid -eq "CRD_CADNO" -or $columnid -eq "SSG_PNT_CADNO" -or $columnid -eq "CST_MPNO" -or $columnid -eq "CST_RRNO" -or $columnid -eq "BRNO" -or $columnid -eq "EGCET_CADNO" -or $columnid -eq "VAN_MNG_EGCET_CADNO" -or $columnid -eq "PCAL_CD_DES" -or $columnid -eq "DTA_EXTR_MTHD_DES" -or $columnid -eq "ACTNO" -or $columnid -eq "ACTNO" -or $columnid -eq "BRNO" -or $columnid -eq "BRNO" -or $columnid -eq "CRD_CADNO" -or $columnid -eq "BRNO" -or $columnid -eq "PIC_TELNO" -or $columnid -eq "PIC_EML_ADDR" -or $columnid -eq "MNFC_COMP_TELNO" -or $columnid -eq "MNFC_COMP_ZIP" -or $columnid -eq "MNFC_COMP_ADDR" -or $columnid -eq "MNFC_COMP_DTLAD" -or $columnid -eq "SAL_COMP_TELNO" -or $columnid -eq "SAL_COMP_ZIP" -or $columnid -eq "SAL_COMP_ADDR" -or $columnid -eq "SAL_COMP_DTLAD" -or $columnid -eq "PIC_TELNO" -or
              $columnid -eq "MNFC_COMP_TELNO" -or $columnid -eq "MNFC_COMP_ZIP" -or $columnid -eq "MNFC_COMP_ADDR" -or $columnid -eq "MNFC_COMP_DTLAD" -or $columnid -eq "SAL_COMP_TELNO" -or $columnid -eq "SAL_COMP_ZIP" -or $columnid -eq "SAL_COMP_ADDR" -or $columnid -eq "SAL_COMP_DTLAD" -or $columnid -eq "PIC_TELNO" -or $columnid -eq "PSIF_ACS_USR_IP_ADDR" -or $columnid -eq "PWD" -or $columnid -eq "BRNO" -or $columnid -eq "CO_INVL_TEL_ARNO" -or $columnid -eq "CO_INVL_TEL_TEXNO" -or $columnid -eq "CO_INVL_TEL_INDNO" -or $columnid -eq "MP_NTNO" -or $columnid -eq "MP_TEXNO" -or $columnid -eq "MP_INDNO" -or $columnid -eq "EML_ADDR" -or $columnid -eq "CI_NO" -or $columnid -eq "PWD" -or $columnid -eq "BRNO" -or $columnid -eq "CO_INVL_TEL_ARNO" -or $columnid -eq "CO_INVL_TEL_TEXNO" -or $columnid -eq "CO_INVL_TEL_INDNO" -or $columnid -eq "MP_NTNO" -or $columnid -eq "MP_TEXNO" -or $columnid -eq "MP_INDNO" -or $columnid -eq "EML_ADDR" -or $columnid -eq "CI_NO" -or $columnid -eq "PWD" -or $columnid -eq "CO_INVL_TEL_ARNO" -or $columnid -eq "CO_INVL_TEL_TEXNO" -or $columnid -eq "CO_INVL_TEL_INDNO" -or $columnid -eq "MP_NTNO" -or $columnid -eq "MP_TEXNO" -or $columnid -eq "MP_INDNO" -or $columnid -eq "EML_ADDR" -or $columnid -eq "CI_NO" -or $columnid -eq "BRNO" -or $columnid -eq "CNNT_IP_ADDR" -or $columnid -eq "RCVR_TELNO" -or $columnid -eq "SDR_TELNO" -or $columnid -eq "CO_INVL_TEL_INDNO" -or
              $columnid -eq "CO_INVL_TEL_ARNO" -or $columnid -eq "CO_INVL_TEL_TEXNO" -or $columnid -eq "HTLN_INQR_PWD" -or $columnid -eq "HTLN_INER_MP_NTNO" -or $columnid -eq "HTLN_INER_MP_TEXNO" -or $columnid -eq "HTLN_INER_MP_INDNO" -or $columnid -eq "HTLN_INER_EML_ADDR" -or $columnid -eq "BRNO" -or $columnid -eq "HO_BRNO" -or $columnid -eq "CRNO" -or $columnid -eq "CPCO_ZIP" -or $columnid -eq "CPCO_ADDR" -or $columnid -eq "CPCO_DTLAD" -or $columnid -eq "HDSTR_ZIP" -or $columnid -eq "HDSTR_ADDR" -or $columnid -eq "HDSTR_DTLAD" -or $columnid -eq "CO_INVL_TEL_ARNO" -or $columnid -eq "CO_INVL_TEL_TEXNO" -or $columnid -eq "CO_INVL_TEL_INDNO" -or $columnid -eq "FAX_ARNO" -or $columnid -eq "FAX_TEXNO" -or $columnid -eq "FAX_INDNO" -or $columnid -eq "CPCO_BRNO" -or $columnid -eq "SSG_PNT_CADNO" -or $columnid -eq "SSG_PNT_CADNO" -or $columnid -eq "SSG_PNT_CADNO" -or $columnid -eq "RCIVR_INVL_TELNO" -or $columnid -eq "SELR_ADDR" -or $columnid -eq "SELR_TELNO" -or $columnid -eq "SELR_ADDR" -or $columnid -eq "SELR_TELNO" -or $columnid -eq "SPLR_BRNO" -or $columnid -eq "SPDR_BRNO" -or $columnid -eq "SPLR_BSC_ADDR" -or $columnid -eq "SPLR_DTLAD" -or $columnid -eq "SPLR_TELNO" -or $columnid -eq "SPLR_EML_ADDR" -or $columnid -eq "SPDR_BSC_ADDR" -or $columnid -eq "SPDR_DTLAD" -or $columnid -eq "SPDR_TELNO" -or $columnid -eq "SPDR_EML_ADDR" -or $columnid -eq "ENCPT_TXBL_UNQ_NO" -or
              $columnid -eq "SPDR_TELNO2" -or $columnid -eq "SPDR_EML_ADDR2" -or $columnid -eq "SPLR_BRNO" -or $columnid -eq "SPDR_BRNO" -or $columnid -eq "SPLR_BSC_ADDR" -or $columnid -eq "SPLR_DTLAD" -or $columnid -eq "SPLR_TELNO" -or $columnid -eq "SPLR_EML_ADDR" -or $columnid -eq "SPDR_BSC_ADDR" -or $columnid -eq "SPDR_DTLAD" -or $columnid -eq "SPDR_TELNO" -or $columnid -eq "SPDR_EML_ADDR" -or $columnid -eq "ENCPT_TXBL_UNQ_NO" -or $columnid -eq "SPDR_TELNO2" -or $columnid -eq "SPDR_EML_ADDR2" -or $columnid -eq "RMC_ACTNO" -or $columnid -eq "BNK_ADDR1" -or $columnid -eq "BNK_ADDR2" -or $columnid -eq "SSGI_XPTR_ADDR" -or $columnid -eq "SSGI_XPTR_EML_ADDR" -or $columnid -eq "BNK_SWFT_CD" -or $columnid -eq "BNK_ACT_IDTF_CD" -or $columnid -eq "BNK_ACTNO" -or $columnid -eq "OVRS_CPCO_ADDR1" -or $columnid -eq "OVRS_CPCO_ADDR2" -or $columnid -eq "OVRS_CPCO_TELNO" -or $columnid -eq "RCIVR_ACTNO" -or $columnid -eq "BNK_SWFT_CD" -or $columnid -eq "BNK_ACT_IDTF_CD" -or $columnid -eq "OVRS_CPCO_ADDR1" -or $columnid -eq "OVRS_CPCO_ADDR2" -or $columnid -eq "OVRS_CPCO_TELNO" -or $columnid -eq "RCIVR_ACTNO" -or $columnid -eq "BNK_SWFT_CD" -or $columnid -eq "BNK_ACT_IDTF_CD" -or $columnid -eq "ACTNO" -or $columnid -eq "BNK_ADDR1" -or $columnid -eq "BNK_ADDR2" -or $columnid -eq "OVRS_CPCO_ADDR1" -or $columnid -eq "OVRS_CPCO_ADDR2" -or $columnid -eq "BNK_SWFT_CD" -or
              $columnid -eq "BRNO" -or $columnid -eq "APLR_TELNO" -or $columnid -eq "SHOP_TELNO1" -or $columnid -eq "SHOP_TELNO2" -or $columnid -eq "SHOP_TELNO3" -or $columnid -eq "PROD_AREA_ADDR" -or $columnid -eq "FATR_ADDR" -or $columnid -eq "BRNO" -or $columnid -eq "BRNO" -or $columnid -eq "CPT_EMP_MPNO" -or $columnid -eq "SHOP_AFSVC_TELNO" -or $columnid -eq "SHOP_REPS_TELNO1" -or $columnid -eq "SHOP_REPS_TELNO2" -or $columnid -eq "SHOP_REPS_TELNO3" -or $columnid -eq "SHOP_REPS_TELNO4" -or $columnid -eq "SHOP_REPS_TELNO5" -or $columnid -eq "COMP_REPS_TELNO1" -or $columnid -eq "COMP_REPS_TELNO2" -or $columnid -eq "SHOP_AFSVC_TELNO" -or $columnid -eq "SHOP_REPS_TELNO1" -or $columnid -eq "SHOP_REPS_TELNO2" -or $columnid -eq "SHOP_REPS_TELNO3" -or $columnid -eq "SHOP_REPS_TELNO4" -or $columnid -eq "SHOP_REPS_TELNO5" -or $columnid -eq "COMP_REPS_TELNO1" -or $columnid -eq "COMP_REPS_TELNO2" -or $columnid -eq "SHOP_AFSVC_TELNO" -or $columnid -eq "SHOP_REPS_TELNO1" -or $columnid -eq "SHOP_REPS_TELNO2" -or $columnid -eq "SHOP_REPS_TELNO3" -or $columnid -eq "SHOP_REPS_TELNO4" -or $columnid -eq "SHOP_REPS_TELNO5" -or $columnid -eq "COMP_REPS_TELNO1" -or $columnid -eq "COMP_REPS_TELNO2" -or $columnid -eq "MPNO" -or $columnid -eq "BRNO" -or $columnid -eq "BRNO" -or $columnid -eq "BRNO" -or $columnid -eq "APLR_TELNO" -or $columnid -eq "SHOP_TELNO" -or $columnid -eq "BTCO_ADDR" -or
              $columnid -eq "SHOP_LXTN_TEL_ARNO" -or $columnid -eq "SHOP_LXTN_TEL_TEXNO" -or $columnid -eq "SHOP_LXTN_TEL_INDNO" -or $columnid -eq "WRKR_MPNO" -or $columnid -eq "EDC_PIC_CO_TELNO" -or $columnid -eq "HRS_PIC_CO_TELNO" -or $columnid -eq "PTR_CETR_TELNO" -or $columnid -eq "SAFE_TEAM_TELNO" -or $columnid -eq "TECH_TEAM_TELNO" -or $columnid -eq "SEC_TEAM_CO_TELNO" -or $columnid -eq "MDOF_CO_TELNO" -or $columnid -eq "HLKP_PIC_CO_TELNO" -or $columnid -eq "MPNO" -or $columnid -eq "MNG_DEPT_TEL_ARNO" -or $columnid -eq "MNG_DEPT_TEL_TEXNO" -or $columnid -eq "MNG_DEPT_TEL_INDNO" -or $columnid -eq "CPCO_BRNO" -or $columnid -eq "CI_NO" -or $columnid -eq "MPNO" -or $columnid -eq "PWD" -or $columnid -eq "CJ_ARLO_SUB_ADDR" -or $columnid -eq "DLVY_CLET_TELNO" -or $columnid -eq "DLVY_CLET_MPNO" -or $columnid -eq "DLVY_CLET_ADDR" -or $columnid -eq "DLVY_CLET_DTLAD" -or $columnid -eq "DLVY_ACPTR_TELNO" -or $columnid -eq "DLVY_ACPTR_MPNO" -or $columnid -eq "DLVY_ACPTR_ADDR" -or $columnid -eq "DLVY_ACPTR_DTLAD" -or $columnid -eq "DLVY_CLET_TELNO" -or $columnid -eq "DLVY_CLET_MPNO" -or $columnid -eq "DLVY_CLET_ZIP" -or $columnid -eq "DLVY_CLET_ADDR" -or $columnid -eq "DLVY_CLET_DTLAD" -or $columnid -eq "DLVY_ACPTR_TELNO" -or $columnid -eq "DLVY_ACPTR_MPNO" -or $columnid -eq "DLVY_ACPTR_ZIP" -or $columnid -eq "DLVY_ACPTR_ADDR" -or $columnid -eq "DLVY_ACPTR_DTLAD" -or
              $columnid -eq "OTP_USR_TELNO" -or $columnid -eq "ENCPT_DLVY_PPLS_DOC_CNTN" -or $columnid -eq "CNNT_IP_ADDR" -or $columnid -eq "CSRS_REQR_TELNO" -or $columnid -eq "CST_TELNO" -or $columnid -eq "CST_ZIP" -or $columnid -eq "CST_ADDR" -or $columnid -eq "CST_DTLAD" -or $columnid -eq "CST_MPNO" -or $columnid -eq "CST_NEW_ZIP" -or $columnid -eq "CORP_CST_TELNO1" -or $columnid -eq "DLVY_CORP_CST_EML_ADDR" -or $columnid -eq "CORP_CST_ZIP" -or $columnid -eq "CORP_CST_ADDR" -or $columnid -eq "CORP_CST_DTLAD" -or $columnid -eq "CORP_CST_TELNO2" -or $columnid -eq "CORP_CST_TELNO3" -or $columnid -eq "DLVY_CETR_ZIP" -or $columnid -eq "DLVY_CETR_ADDR" -or $columnid -eq "DLVY_CETR_REPS_TELNO1" -or $columnid -eq "DLVY_CETR_REPS_TELNO2" -or $columnid -eq "DLVY_CETR_FAXNO" -or $columnid -eq "DLVY_CETR_EML_ADDR" -or $columnid -eq "DLVY_VHC_DRVR_TELNO" -or $columnid -eq "DLVY_BFTF_RCVD_USR_PWD" -or $columnid -eq "DLVY_CLET_TELNO" -or $columnid -eq "DLVY_CLET_MPNO" -or $columnid -eq "DLVY_CLET_ZIP" -or $columnid -eq "DLVY_CLET_ADDR" -or $columnid -eq "DLVY_CLET_DTLAD" -or $columnid -eq "DLVY_ACPTR_TELNO" -or $columnid -eq "DLVY_ACPTR_MPNO" -or $columnid -eq "DLVY_ACPTR_ZIP" -or $columnid -eq "DLVY_ACPTR_ADDR" -or $columnid -eq "DLVY_ACPTR_DTLAD" -or $columnid -eq "DLVY_BFTF_RCVD_USR_TELNO" -or $columnid -eq "DLVY_BFTF_RCVD_USR_PWD" -or $columnid -eq "DLVY_VHC_DRVR_TELNO" -or
              $columnid -eq "DLVY_CLET_TELNO" -or $columnid -eq "DLVY_CLET_MPNO" -or $columnid -eq "DLVY_ACPTR_TELNO" -or $columnid -eq "DLVY_ACPTR_MPNO" -or $columnid -eq "DLVY_ACPTR_ZIP" -or $columnid -eq "DLVY_ACPTR_ADDR" -or $columnid -eq "DLVY_ACPTR_DTLAD" -or $columnid -eq "DLVY_CLET_TELNO" -or $columnid -eq "DLVY_CLET_MPNO" -or $columnid -eq "DLVY_CLET_ZIP" -or $columnid -eq "DLVY_CLET_ADDR" -or $columnid -eq "DLVY_CLET_DTLAD" -or $columnid -eq "DLVY_CLET_EML_ADDR" -or $columnid -eq "DLVY_REQ_AGT_TELNO" -or $columnid -eq "DLVY_ACPTR_TELNO" -or $columnid -eq "DLVY_ACPTR_MPNO" -or $columnid -eq "DLVY_ACPTR_ZIP" -or $columnid -eq "DLVY_ACPTR_ADDR" -or $columnid -eq "DLVY_ACPTR_DTLAD" -or $columnid -eq "SAL_CRN_TELNO" -or $columnid -eq "CNNT_IP_ADDR" -or $columnid -eq "DLCO_DB_CNNT_PWD" -or $columnid -eq "DLVY_CLET_TELNO" -or $columnid -eq "DLVY_CLET_MPNO" -or $columnid -eq "DLVY_CLET_ZIP" -or $columnid -eq "DLVY_CLET_ADDR" -or $columnid -eq "DLVY_CLET_DTLAD" -or $columnid -eq "DLVY_ACPTR_TELNO" -or $columnid -eq "DLVY_ACPTR_MPNO" -or $columnid -eq "DLVY_ACPTR_ZIP" -or $columnid -eq "DLVY_ACPTR_ADDR" -or $columnid -eq "DLVY_ACPTR_DTLAD" -or $columnid -eq "DLVY_ACPTR_TELNO" -or $columnid -eq "DLVY_ACPTR_TELNO" -or $columnid -eq "DLVY_ACPTR_TELNO" -or $columnid -eq "DLVY_NOTFI_RCVR_TELNO" -or $columnid -eq "DLVY_CST_SMS_RCV_TELNO" -or $columnid -eq "SSG_PNT_CADNO" -or
              $columnid -eq "SSG_PNT_CADNO" -or $columnid -eq "CRD_CADNO" -or $columnid -eq "RCP_MARK_CCAD_FRNT_NO" -or $columnid -eq "RCP_MARK_CCAD_BKNO_NO" -or $columnid -eq "SSGPY_VRTL_BNK_ACTNO" -or $columnid -eq "SSGPY_RCP_MARK_VRTL_ACTNO" -or $columnid -eq "SSG_PNT_CADNO" -or $columnid -eq "CRD_CADNO" -or $columnid -eq "ACCET_BRNO" -or $columnid -eq "ACCET_TELNO" -or $columnid -eq "BRNO" -or $columnid -eq "SSG_PNT_CADNO" -or $columnid -eq "SSG_PNT_CADNO" -or $columnid -eq "EGCET_CADNO" -or $columnid -eq "SHOP_BRNO" -or $columnid -eq "CRD_CADNO" -or $columnid -eq "BRNO" -or $columnid -eq "KKO_PNT_CADNO" -or $columnid -eq "OTC_CADNO" -or $columnid -eq "CASH_IC_CADNO" -or $columnid -eq "CASH_RCP_ISSU_NO" -or $columnid -eq "CST_MPNO" -or $columnid -eq "CST_MPNO" -or $columnid -eq "CST_TELNO" -or $columnid -eq "CST_MPNO" -or $columnid -eq "CST_ZIP" -or $columnid -eq "CST_ADDR" -or $columnid -eq "CST_DTLAD" -or $columnid -eq "CST_TELNO" -or $columnid -eq "CST_MPNO" -or $columnid -eq "CST_ZIP" -or $columnid -eq "CST_ADDR" -or $columnid -eq "CST_DTLAD" -or $columnid -eq "CST_MPNO" -or $columnid -eq "CST_MPNO" -or $columnid -eq "CST_MPNO" -or $columnid -eq "CST_MPNO" -or $columnid -eq "CRD_CADNO" -or $columnid -eq "CRD_CADNO" -or $columnid -eq "SSG_PNT_CADNO" -or $columnid -eq "AFLT_CHNL_PIC_TELNO" -or $columnid -eq "CST_MPNO" -or $columnid -eq "HSHM_CADNO" -or $columnid -eq "CST_MPNO" -or $columnid -eq "CST_MPNO" -or
              $columnid -eq "BRDT" -or $columnid -eq "GNDR_DVS_CD" -or $columnid -eq "APPR_NM" -or $columnid -eq "IMP_SUPT_CFMT_UPDR_NM" -or $columnid -eq "AFLT_CHNL_PIC_NM" -or $columnid -eq "CST_NM" -or $columnid -eq "REPR_NM" -or $columnid -eq "RCP_NMPL_BZMN_NM" -or $columnid -eq "RCVD_EMP_NM" -or $columnid -eq "DLVY_VHC_NO" -or $columnid -eq "DLVY_VHC_DRVR_NM" -or $columnid -eq "DLVY_CLET_NM" -or $columnid -eq "DLVY_REQ_AGT_NM" -or $columnid -eq "DLVY_ACPTR_NM" -or $columnid -eq "PSPT_ENGH_NM" -or $columnid -eq "ISRF_PSPT_GNDR_DVS_CD" -or $columnid -eq "ISRF_PSPT_BRDT") {
              continue    
          }

          if ($nullable -eq "NO") {
              $nullable = "NOT NULL"
          } else {
              $nullable = "NULL"
          }

          if ($datatype -eq "datetime") {
              $datatype = "TIMESTAMP"
          } elseif ($datatype -eq "decimal") {
              $datatype = "NUMERIC"
          } elseif ($datatype -eq "int") {
              $datatype = "INTEGER"
          } elseif ($datatype -eq "longtext") {
              continue
          } else {
              $datatype = $datatype
          }

          if ($dataType -eq "VARCHAR") {
              $dataLength4 = $maxlength*4
          } else {
              $dataLength4 = $maxlength
          }

          $worksheet.Cells.Item($sorA, 12).Value2 = $Owner
          $worksheet.Cells.Item($sorA, 13).Value2 = $tableid.ToUpper()
          $worksheet.Cells.Item($sorA, 14).Value2 = $tablename
          $worksheet.Cells.Item($sorA, 15).Value2 = $columnid.ToUpper()
          $worksheet.Cells.Item($sorA, 16).Value2 = $columnname
          $worksheet.Cells.Item($sorA, 17).Value2 = $datatype.ToUpper()
          $worksheet.Cells.Item($sorA, 18).Value2 = $dataLength4.ToString()
          $worksheet.Cells.Item($sorA, 19).Value2 = $precision.ToString()
          $worksheet.Cells.Item($sorA, 20).Value2 = $scale.ToString()
          $worksheet.Cells.Item($sorA, 21).Value2 = $nullable

          $sorA = $sorA + 1
          $resultcnt = $resultcnt + 1
         } #While문 끝                         
         if ($redresultcnt -lt $resultcnt) {
             $cnt = $resultcnt - $redresultcnt
             for($addnum=1; $addnum -le $cnt; $addnum++) {
                 $worksheet.Cells.Item($tarA, 2).Value2 = "컬럼 추가"
                 $tarA = $tarA + 1
             }
             $sorA = $tarA
         } elseif ($redresultcnt -gt $resultcnt) {
             $cnt = $redresultcnt - $resultcnt
             for($addnum=1; $addnum -le $cnt; $addnum++) {
                 $worksheet.Cells.Item($sorA, 12).Value2 = "컬럼 삭제"
                 $sorA = $sorA + 1
             }
             $tarA = $sorA
         }
     }
     $rowcount=$worksheet.UsedRange.Rows.Count
     for ($j=2; $j -le $rowcount; $j++) {
         $worksheet.Cells.Item($j, 22).Value2 = "=IF(C2&D2&E2&F2&G2&J2&K2 = `"S`"&SUBSTITUTE(M2,`"$`",`"`")&`"S`"&N2&O2&P2&Q2&T2&U2,`"TRUE`",`"FALSE`")"
     }
     Remove-Item $Path
     $workbook.SaveAs($Path)
     $ExcelWorkBook.Close()
     $workbook.Close()
     $ExcelObj.Quit()
     $excel.Quit()
} catch {
     Write-Error ("Database Exception:{0}{1}" -f ` $con.ConnectionString, $_.Exception.ToString())
} finally{
     if ($connection.State -eq 'Open' -or $redconnection.State -eq 'Open') {
        $connection.close()
        $redconnection.close()
     }
}