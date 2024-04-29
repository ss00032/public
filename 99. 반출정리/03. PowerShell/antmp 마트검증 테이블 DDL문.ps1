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
     $ExcelWorkSheet = $ExcelWorkBook.Sheets.Item("분석_보고서마트 테이블목록")
     #$ExcelWorkSheet = $ExcelWorkBook.Sheets.Item("test")

     $rowcount=$ExcelWorkSheet.UsedRange.Rows.Count

     $DDLALL=@()

     for ($i=2; $i -le $rowcount; $i++) {
         $ExceltableID = $ExcelWorkSheet.cells.Item($i, 2).value2
         $ExceltableNM = $ExcelWorkSheet.cells.Item($i, 3).value2

         $ExceltableArr=@()

         Write-Host "--------------------" $ExceltableID".("$ExceltableNM")"

         $ExceltableArr += "--------------------------------------------------------------------------------"
         $ExceltableArr += "-- 적용시스템명 : 개발계(RedShift)"
         $ExceltableArr += "-- 테이블 : " + $ExceltableNM + "_검증"
         $ExceltableArr += "-- 테이블ID : " + $ExceltableID + "_TVF"
         $ExceltableArr += "--------------------------------------------------------------------------------"
         $ExceltableArr += "DROP TABLE IF EXISTS antmp." + $ExceltableID + "_TVF;"
         $ExceltableArr += "CREATE TABLE antmp." + $ExceltableID + "_TVF ("
         $ExceltableArr += "  CRI_YMD                        VARCHAR(32)     NOT NULL"
         $ExceltableArr += ", TLE_ID                         VARCHAR(280)    NOT NULL"
         $ExceltableArr += ", CLMN_ID                        VARCHAR(800)    NOT NULL"
         $ExceltableArr += ", ETL_WRK_DTM                    TIMESTAMP       "
         $ExceltableArr += ", TEST_ID                        VARCHAR(200)    "
         $ExceltableArr += ", TLE_NM                         VARCHAR(400)    "
         $ExceltableArr += ", CLMN_NM                        VARCHAR(2000)   "
         $ExceltableArr += ", SRCE_TLE_ID                    VARCHAR(280)    "
         $ExceltableArr += ", SRCE_TLE_NM                    VARCHAR(400)    "
         $ExceltableArr += ", SRCE_CLMN_ID                   VARCHAR(800)    "
         $ExceltableArr += ", SRCE_CLMN_NM                   VARCHAR(2000)   "
         $ExceltableArr += ", TGT_VFCT_VAL                   BIGINT          "
         $ExceltableArr += ", SRCE_VFCT_VAL                  BIGINT          "
         $ExceltableArr += ");"
         $ExceltableArr += ""
         $ExceltableArr += "COMMENT ON TABLE antmp." + $ExceltableID + "_TVF IS '" + $ExceltableNM + "_검증';"
         $ExceltableArr += "COMMENT ON COLUMN antmp." + $ExceltableID + "_TVF.CRI_YMD IS '기준일자';"
         $ExceltableArr += "COMMENT ON COLUMN antmp." + $ExceltableID + "_TVF.TLE_ID IS '테이블ID';"
         $ExceltableArr += "COMMENT ON COLUMN antmp." + $ExceltableID + "_TVF.CLMN_ID IS '컬럼ID';"
         $ExceltableArr += "COMMENT ON COLUMN antmp." + $ExceltableID + "_TVF.ETL_WRK_DTM IS 'ETL작업일시';"
         $ExceltableArr += "COMMENT ON COLUMN antmp." + $ExceltableID + "_TVF.TEST_ID IS '테스트ID';"
         $ExceltableArr += "COMMENT ON COLUMN antmp." + $ExceltableID + "_TVF.TLE_NM IS '테이블명';"
         $ExceltableArr += "COMMENT ON COLUMN antmp." + $ExceltableID + "_TVF.CLMN_NM IS '컬럼명';"
         $ExceltableArr += "COMMENT ON COLUMN antmp." + $ExceltableID + "_TVF.SRCE_TLE_ID IS '원천테이블ID';"
         $ExceltableArr += "COMMENT ON COLUMN antmp." + $ExceltableID + "_TVF.SRCE_TLE_NM IS '원천테이블명';"
         $ExceltableArr += "COMMENT ON COLUMN antmp." + $ExceltableID + "_TVF.SRCE_CLMN_ID IS '원천컬럼ID';"
         $ExceltableArr += "COMMENT ON COLUMN antmp." + $ExceltableID + "_TVF.SRCE_CLMN_NM IS '원천컬럼명';"
         $ExceltableArr += "COMMENT ON COLUMN antmp." + $ExceltableID + "_TVF.TGT_VFCT_VAL IS '타겟검증값';"
         $ExceltableArr += "COMMENT ON COLUMN antmp." + $ExceltableID + "_TVF.SRCE_VFCT_VAL IS '원천검증값';"
         $DDLALL += $ExceltableArr
         $ExceltableArr > C:\Users\Owner\Documents\source\antmp\antmptable\$ExceltableID"_TVF.sql"
     }
     $DDLALL > C:\Users\Owner\Documents\source\antmp\antmpall.sql
     $ExcelWorkBook.Close()

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