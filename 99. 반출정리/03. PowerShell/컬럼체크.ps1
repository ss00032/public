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
         #Write-Host "--------------------" $ExcelOwner"."$ExcelTable

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

             if($ExcelTable -eq "SFM_ZACT1530" -or $ExcelTable -eq "SFM_ZPCT0001" -or $ExcelTable -eq "SFM_ZPCT0002") {
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
              if ($Excelchgyn -eq 'Y') {
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
             # 분석계 레이아웃을 불러오기 위한 redshift 연결
             $redconnection = New-Object System.Data.Odbc.OdbcConnection($redconnectionString)
             $redconnection.Open()

    
             $command = New-Object System.Data.Odbc.OdbcCommand($statement, $redconnection)
             $command.CommandText = $statement
             $result = $command.ExecuteReader()
             $strshopchk = '1'
             $mdchk = '1'
    
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

              if ($pkyn -eq '1' -and ($columnid -eq "STR_CD" -or $columnid -eq "SHOP_CD")) {
                  $strshopchk = '0'
              }

              if ($columnid -eq "MD_CD") {
                  $mdchk = '0'
              }
             } #While문 끝
             if ($strshopchk -eq '0' -and $mdchk -eq '0') {
                  Write-Host $ExcelTable","$ExcelTableNm
              }
         }
     }
     $ExcelWorkBook.Close()
     #$connection.close()
     $redconnection.close()
     Write-Host "=================================================================================================="
     Write-Host "=============================================작업완료============================================="
     Write-Host "=================================================================================================="

} catch {
     Write-Error ("Database Exception:{0}{1}" -f ` $con.ConnectionString, $_.Exception.ToString())
} finally{
     if ($redconnection.State -eq 'Open') {
        $redconnection.close()
     }
}