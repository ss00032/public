Add-Type -AssemblyName System.Data.OracleClient

$PSDefaultParameterValues['*:Encoding'] = 'Default'

# 운영계 통합운영정보
$driver = "MySQL ODBC 8.0 ANSI Driver"
$server = "dept-rds-an2-op-dev-opmdsdbd.cluster-c92dc3uwdt0o.ap-northeast-2.rds.amazonaws.com"
$username = "MDSDEV"
$password = "md`$D2v2023"
$data_source = "MDSDEV"
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

     #$ExcelWorkBook = $ExcelObj.Workbooks.Open("C:\Users\Owner\Desktop\신세계백화점 차세대 시스템 구축\03. 개발\99. 관리문서\테이블정의서(SOR)_DDL작업용.xlsx")
     $ExcelWorkBook = $ExcelObj.Workbooks.Open("C:\Users\Owner\Desktop\신세계백화점 차세대 시스템 구축\03. 개발\99. 관리문서\테이블정의서(마트)_DDL작업용.xlsx")
     $ExcelWorkSheet = $ExcelWorkBook.Sheets.Item("테이블정의서")
     
     $rowcount=$ExcelWorkSheet.UsedRange.Rows.Count

     $createDDL=@()
     $createDDLAll=@()
     $createstgDDL=@()
     $createstgDDLAll=@()
     $columnArr=@()
     $columnNameArr=@()

     $martgubun=""

     for ($i=2; $i -le $rowcount; $i++) {
         $j = $i - 1
         $tableID = $ExcelWorkSheet.cells.Item($i, 2).value2
         $tableID1 = $ExcelWorkSheet.cells.Item($j, 2).value2
         #$stgcomtable = $tableID.Substring(1, $tableID.Length-1)
         $stgcomtable = $tableID + "_T01"
         #$stgtable = $tableID1.Substring(1, $tableID1.Length-1)
         $stgtable = $tableID1 + "_T01"
         $tableName = $ExcelWorkSheet.cells.Item($i, 3).value2
         $tableName1 = $ExcelWorkSheet.cells.Item($j, 3).value2
         #$stgtableName = $tableName1.Substring(1, $tableName1.Length-1)
         $stgtableName = $tableName1 + "_T01"
         $columnID = $ExcelWorkSheet.cells.Item($i, 4).value2
         $columnName = $ExcelWorkSheet.cells.Item($i, 5).value2
         $position = $ExcelWorkSheet.cells.Item($i, 6).value2
         $dataType = $ExcelWorkSheet.cells.Item($i, 7).value2
         $dataLength = $ExcelWorkSheet.cells.Item($i, 8).value2
         $nullAble = $ExcelWorkSheet.cells.Item($i, 9).value2
         $pkyn = $ExcelWorkSheet.cells.Item($i, 10).value2
         $default = $ExcelWorkSheet.cells.Item($i, 11).value2
         $gubun = $ExcelWorkSheet.cells.Item($i, 12).value2

         if ($tableID -eq $tableID1) {
             
         } else {
             if ($j -eq 1) {
                
             } else {
                 Write-Host "================" $tableID1
                 $createDDL += "--------------------------------------------------------------------------------"
                 $createDDL += "-- 적용시스템명 : 개발계(RedShift)"
                 $createDDL += "-- 테이블 : " + $tableName1
                 $createDDL += "-- 테이블ID : " + $tableID1
                 $createDDL += "--------------------------------------------------------------------------------"
                 $createDDL += "DROP TABLE IF EXISTS " + $martgubun + "."  + $tableID1 + ";"
                 $createDDL += "CREATE TABLE " + $martgubun + "." + $tableID1 + " ("
                 $createDDL += $columnArr
                 $createDDL += ");"
                 $createDDL += ""
                 $createDDL += "COMMENT ON TABLE " + $martgubun + "." + $tableID1 + " IS '" + $tableName1 + "';"
                 $createDDL += $columnNameArr
                 $createDDL += ""
                 #$createDDL += "grant select on anana." + $tableID1 + " to group select_dept_dw_group ;"
                 #$createDDL += "grant all on anana." + $tableID1 + " to group all_dept_dw_group ;"
                 $createDDLAll += $createDDL
                 $createDDL > C:\Users\Owner\Documents\source\DDL\table\$tableID1.sql

                 if ($tableName1.Contains("스냅샷")) {

                 } else {
                     $createstgDDL += "--------------------------------------------------------------------------------"
                     $createstgDDL += "-- 적용시스템명 : 개발계(RedShift)"
                     $createstgDDL += "-- 테이블 : " + $stgtableName
                     $createstgDDL += "-- 테이블ID : " + $stgtable
                     $createstgDDL += "--------------------------------------------------------------------------------"
                     $createstgDDL += "DROP TABLE IF EXISTS antmp."  + $stgtable + ";"
                     $createstgDDL += "CREATE TABLE antmp." + $stgtable + " ("
                     $createstgDDL += $columnArr
                     $createstgDDL += ");"
                     $createstgDDL += ""
                     $createstgDDL += "COMMENT ON TABLE antmp." + $stgtable + " IS '" + $stgtableName + "';"
                     $createstgDDL += $columnNamestgArr
                     $createstgDDL += ""
                     #$createstgDDL += "grant select on antmp." + $stgtable + " to group select_dept_dw_group ;"
                     #$createstgDDL += "grant all on antmp." + $stgtable + " to group all_dept_dw_group ;"
                     $createstgDDLAll += $createstgDDL
                     $createstgDDL > C:\Users\Owner\Documents\source\DDL\table\stg\$stgtable.sql
                 }
             }
             $createDDL=@()
             $createstgDDL=@()
             $columnArr=@()
             $columnNameArr=@()
             $columnNamestgArr=@()
             if ($gubun -eq "분석마트") {
                 $martgubun = "anana"
             } elseif ($gubun -eq "보고서마트") {
                 $martgubun = "anrep"
             }
         }
         if ($dataType -eq "VARCHAR") {
             $dataLength4 = $dataLength*4
         } else {
             $dataLength4 = $dataLength
         }
         if ($dataType -eq "VARCHAR" -or $dataType -eq "CHAR") {
             if ($dataLength -eq "") {
                 $dataType = $dataType
             } else {
                 $dataType = $dataType + "(" + $dataLength4 + ")"
             }
         } elseif ($dataType -eq "DECIMAL") {
             if ($dataLength -eq "") {
                 $dataType = "NUMERIC"
             } else {
                 $dataType = "NUMERIC" + "(" + $dataLength4 + ")"
             }
         } elseif ($dataType -eq "NUMERIC") {
             if ($dataLength -eq "") {
                 $dataType = "NUMERIC"
             } else {
                 $dataType = "NUMERIC" + "(" + $dataLength4 + ")"
             }
         } elseif ($dataType -eq "INT") {
             $dataType = "INTEGER"
         } elseif ($dataType -eq "BIGINT" -or $dataType -eq "date" -or $dataType -eq "INTEGER" -or $dataType -eq "SMALLINT" -or $dataType -eq "TIMESTAMP" -or $dataType -eq "timestampz") {
             $dataType = $dataType
         }

         #if ($default -is [double]) {
         #    Write-Host "숫자 타입입니다."
         #} elseif ($default -is [string]) {
         #    Write-Host $default " 텍스트 타입입니다."
         #} else {
         #    Write-Host "다른 유형입니다."
         #}
         if ($position -eq 1) {
         $columnArr += "  " + $columnID.PadRight(30) + " " + $dataType.PadRight(15) + " " + $nullAble
         #    if ($default -is [double]) {
         #        $columnArr += "  " + $columnID.PadRight(30) + " " + $dataType.PadRight(15) + " " + $nullAble.PadRight(5) + ("  DEFAULT " + $default)
         #    } elseif ($default -is [string]) {
         #        $columnArr += "  " + $columnID.PadRight(30) + " " + $dataType.PadRight(15) + " " + $nullAble.PadRight(5) + ("  DEFAULT " + $default)
         #    } else {
         #        $columnArr += "  " + $columnID.PadRight(30) + " " + $dataType.PadRight(15) + " " + $nullAble
         #    }
         } else {
         $columnArr += ", " + $columnID.PadRight(30) + " " + $dataType.PadRight(15) + " " + $nullAble
         #    if ($default -is [double]) {
         #        $columnArr += ", " + $columnID.PadRight(30) + " " + $dataType.PadRight(15) + " " + $nullAble.PadRight(5) + ("  DEFAULT " + $default)
         #    } elseif ($default -is [string]) {
         #        $columnArr += ", " + $columnID.PadRight(30) + " " + $dataType.PadRight(15) + " " + $nullAble.PadRight(5) + ("  DEFAULT " + $default)
         #    } else {
         #        $columnArr += ", " + $columnID.PadRight(30) + " " + $dataType.PadRight(15) + " " + $nullAble
         #    }
         } 

         $columnNameArr += "COMMENT ON COLUMN " + $martgubun + "." + $tableID + "." + $columnID + " IS '" + $columnName + "';"
         $columnNamestgArr += "COMMENT ON COLUMN antmp." + $stgcomtable + "." + $columnID + " IS '" + $columnName + "';"

         if ($i -eq $rowcount) {
             $createDDL += "--------------------------------------------------------------------------------"
             $createDDL += "-- 적용시스템명 : 개발계(RedShift)"
             $createDDL += "-- 테이블 : " + $tableName1
             $createDDL += "-- 테이블ID : " + $tableID1
             $createDDL += "--------------------------------------------------------------------------------"
             $createDDL += "DROP TABLE IF EXISTS " + $martgubun + "."  + $tableID1 + ";"
             $createDDL += "CREATE TABLE " + $martgubun + "." + $tableID1 + " ("
             $createDDL += $columnArr
             $createDDL += ");"
             $createDDL += ""
             $createDDL += "COMMENT ON TABLE " + $martgubun + "." + $tableID1 + " IS '" + $tableName1 + "';"
             $createDDL += $columnNameArr
             $createDDL += ""
             #$createDDL += "grant select on anana." + $tableID1 + " to group select_dept_dw_group ;"
             #$createDDL += "grant all on anana." + $tableID1 + " to group all_dept_dw_group ;"
             $createDDLAll += $createDDL
             $createDDL > C:\Users\Owner\Documents\source\DDL\table\$tableID1.sql

             if ($tableName1.Contains("스냅샷")) {

             } else {
                 $createstgDDL += "--------------------------------------------------------------------------------"
                 $createstgDDL += "-- 적용시스템명 : 개발계(RedShift)"
                 $createstgDDL += "-- 테이블 : " + $stgtableName
                 $createstgDDL += "-- 테이블ID : " + $stgtable
                 $createstgDDL += "--------------------------------------------------------------------------------"
                 $createstgDDL += "DROP TABLE IF EXISTS antmp."  + $stgtable + ";"
                 $createstgDDL += "CREATE TABLE antmp." + $stgtable + " ("
                 $createstgDDL += $columnArr
                 $createstgDDL += ");"
                 $createstgDDL += ""
                 $createstgDDL += "COMMENT ON TABLE antmp." + $stgtable + " IS '" + $stgtableName + "';"
                 $createstgDDL += $columnNamestgArr
                 $createstgDDL += ""
                 #$createstgDDL += "grant select on antmp." + $stgtable + " to group select_dept_dw_group ;"
                 #$createstgDDL += "grant all on antmp." + $stgtable + " to group all_dept_dw_group ;"
                 $createstgDDLAll += $createstgDDL
                 $createstgDDL > C:\Users\Owner\Documents\source\DDL\table\stg\$stgtable.sql
             }
         }
     }
     $createDDLAll > C:\Users\Owner\Documents\source\DDL\tableALL_mart.sql
     $createstgDDLAll > C:\Users\Owner\Documents\source\DDL\stgtableALL_mart.sql

     $ExcelWorkBook.Close()
     $ExcelObj.Quit()
} catch {
     Write-Error ("Database Exception:{0}{1}" -f ` $con.ConnectionString, $_.Exception.ToString())
} finally{
     if ($connection.State -eq 'Open' -or $redconnection.State -eq 'Open') {
        $connection.close()
        $redconnection.close()
     }
}