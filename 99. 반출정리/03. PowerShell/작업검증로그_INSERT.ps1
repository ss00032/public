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
$redusername = "etladmin"
$redpassword = "EtlAdmin1!!"
$redconnectionString = "Driver={Amazon Redshift (x64)}; Server=$redhost; Port=$redport; Database=$reddbName; UID=$redusername; PWD=$redpassword;"

try {
     #작업 목록
     $ExcelObj = new-Object -Comobject Excel.Application
     $ExcelObj.visible=$false

     $ExcelWorkBook = $ExcelObj.Workbooks.Open("C:\Users\Owner\Desktop\신세계백화점 차세대 시스템 구축\03. 개발\99. 관리문서\SSGDX_A_AN_TOBE-SOR 테이블 목록.xlsx")
     $ExcelWorkSheet = $ExcelWorkBook.Sheets.Item("SOR 목록")
     #$ExcelWorkSheet = $ExcelWorkBook.Sheets.Item("IF 목록")
     #$ExcelWorkSheet = $ExcelWorkBook.Sheets.Item("test")

     $rowcount=$ExcelWorkSheet.UsedRange.Rows.Count
     
     #제외 대상 컬럼 목록
     #$ExcelObj1 = new-Object -Comobject Excel.Application
     #$ExcelObj1.visible=$false

     #$ExcelWorkBook1 = $ExcelObj1.Workbooks.Open("C:\Users\Owner\Desktop\신세계백화점 차세대 시스템 구축\03. 개발\99. 관리문서\SSGDX_A_AN_설계준비(컬럼삭제대상컬럼목록).xlsx")
     #$ExcelWorkSheet1 = $ExcelWorkBook1.Sheets.Item("Sheet1")
     
     #$rowcount1=$ExcelWorkSheet1.UsedRange.Rows.Count
     #$dsjoball=@()

     #이행대상 테이블 기준 키
     $implExcelObj1 = new-Object -Comobject Excel.Application
     $implExcelObj1.visible=$false

     $implExcelWorkBook = $implExcelObj1.Workbooks.Open("C:\Users\Owner\Desktop\신세계백화점 차세대 시스템 구축\03. 개발\99. 관리문서\적재건수별그룹핑작업(실행스크립트그룹)_20230829.xlsx")
     $implExcelWorkSheet = $implExcelWorkBook.Sheets.Item("테이블목록")
     
     $implrowcount=$implExcelWorkSheet.UsedRange.Rows.Count

     $LI_sel_statement = @()
     $LC_sel_statement = @()
     $statement = @()
     $imple_sel = @()
     $impl_sel_stat = @()
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

         $impleChk = "N"
         $chgYN = 'N'   # 변경적재 여부에 따라 실행 쿼리 다르게 Default Y(변경적재) <-> N(초기적재)
         $chgKey = ""

         #if ($Exceljuje -eq "SAP") {
         #    continue
         #}

         if ($Excelasistable -eq "X") {
             continue
         } else {
             $ExternalTable = $Excelasistable
             $TableLength = $ExcelTable.Length-1
             $TableNmLength = $ExcelTableNm.Length-1
             if ($Exceljuje -eq "17.재무관리" -or $Exceljuje -eq "SAP" -or $Exceljuje -eq "SCRM") {
                 $TempTable = $Excelasistable
                 $TempTableNm = $Excelasistbnm
             } else {
                 $TempTable = $ExcelTable.Substring(1, $TableLength)
                 $TempTableNm = $ExcelTableNm.Substring(1, $TableNmLength)
             }
             $TempOwner = "ANSTG"
         }
         Write-Host "--------------------" $ExcelOwner"."$ExcelTable

         if ($Excelchgyn -eq 'S') {
             $TempOwner = $Excelasisowner
             $TempTable = $Excelasistable
             $TempTableNm = $Excelasistbnm
         } else {
             $Excelredowner = $ExcelOwner
             $Excelredtable = $ExcelTable
         }

         if ($Excelasisowner -eq 'ext_mds') {
             $Excelasisowner = 'MDSDEV'
         }

         #$cri_ymd = '20230803'
         if ($ExcelTable -eq "SSA_SALS_TRAN_PCAL_P" -or $ExcelTable -eq "SMK_TRAN_DCNT_EVN_HDER_P" -or $ExcelTable -eq "SIV_PIPD_RCDS_BYDY_S" -or $ExcelTable -eq "SSA_SALS_VIP_MLG_P" -or $ExcelTable -eq "SIV_TDMR_BYDY_MD_S" -or 
             $ExcelTable -eq "SSA_SALS_CASH_P" -or $ExcelTable -eq "SSS_PIPD_BYMR_SALS_S" -or $ExcelTable -eq "SIV_PIPD_RCDS_BYMN_S" -or $ExcelTable -eq "SSS_BYMD_BYMR_SALS_S" -or $ExcelTable -eq "SSS_BYCAHR_BYMD_BYMR_SALS_S" -or
             $ExcelTable -eq "SSA_SALS_HDER_P" -or $ExcelTable -eq "SMK_TRAN_DCNT_APY_P" -or $ExcelTable -eq "SSA_SALS_PDCT_P" -or $ExcelTable -eq "SMK_MRKT_EVN_D" -or $ExcelTable -eq "SSA_SALS_GFCET_P" -or $ExcelTable -eq "SIV_GRPST_DTL" -or 
             $ExcelTable -eq "SPO_ORDE_DTL" -or $ExcelTable -eq "SPO_PCH_DTL" -or $ExcelTable -eq "SMK_TRAN_DCNT_EVN_APY_P" -or $ExcelTable -eq "SIV_TDMR_BYMN_MD_S" -or $ExcelTable -eq "SSA_SALS_SSG_PNT_SLMG_P" -or $ExcelTable -eq "SSA_SALS_CAD_P" -or 
             $ExcelTable -eq "SPF_SALS_BYDY_BYMD_BYSTMN" -or $ExcelTable -eq "SSA_SALS_SSG_PNT_P" -or $ExcelTable -eq "SPR_SHOP_PIPD_PRC" -or $ExcelTable -eq "SMK_MRKT_EVN_STR_MD_D" -or $ExcelTable -eq "SPR_NPPD_INFO_SHOP_PRC" -or 
             $ExcelTable -eq "SIV_ANYT_INVT_SUV" -or $ExcelTable -eq "SMK_TRAN_DCNT_EVN_DTL_ITEM_P"  -or $ExcelTable -eq "SSA_ORD_ONOFL_ORD_P") {
             $impleChk = "Y"
             for ($a=4; $a -le $implrowcount; $a++) {
                 $implExcelTable = $implExcelWorkSheet.cells.Item($a, 4).value2
                 $implChgKey = $implExcelWorkSheet.cells.Item($a, 11).value2
                 if($ExcelTable -eq $implExcelTable) {
                     $chgKey = $implChgKey
                     continue
                 }
             }
             Write-Host "1"
         }

         if ($impleChk -eq "Y") {
             $imple_sel += "/**************************************************************************************************/"
             $imple_sel += "/* P_BAT_STG_NM : Target Insert                                                                   */"
             $imple_sel += "/**************************************************************************************************/"
             $imple_sel += "INSERT INTO ANSOR.SCM_BAT_WRK_VFCT_L"
             $imple_sel += "     ("
             $imple_sel += "       CRI_YMD"
             $imple_sel += "     , TLE_ID"
             $imple_sel += "     , CLMN_ID"
             $imple_sel += "     , ETL_WRK_DTM"
             $imple_sel += "     , TEST_ID"
             $imple_sel += "     , TLE_NM"
             $imple_sel += "     , CLMN_NM"
             $imple_sel += "     , SRCE_TLE_ID"
             $imple_sel += "     , SRCE_TLE_NM"
             $imple_sel += "     , SRCE_CLMN_ID"
             $imple_sel += "     , SRCE_CLMN_NM"
             $imple_sel += "     , TGT_VFCT_VAL"
             $imple_sel += "     , SRCE_VFCT_VAL"
             $imple_sel += "     )"
             $imple_sel += "SELECT '{BASE_DT_TO}' AS 기준일자"
             $imple_sel += "     , '" + $ExcelTable + "' AS 테이블ID"
             $imple_sel += "     , 'COUNT(1)' AS 컬럼ID" 
             $imple_sel += "     , CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' AS ETL작업일시"
             $imple_sel += "     , 'UT_" + $ExcelTable + "_01' AS 테스트ID"
             $imple_sel += "     , '" + $ExcelTableNm + "' AS 테이블명"
             $imple_sel += "     , '모수검증' AS 컬럼명"
             $imple_sel += "     , '" + $Excelasistable +"' AS 원천테이블ID"
             $imple_sel += "     , '" + $Excelasistbnm + "' AS 원천테이블명"
             $imple_sel += "     , 'COUNT(1)' AS 원천컬럼ID"
             $imple_sel += "     , '모수검증' AS 원천컬럼명"
             $imple_sel += "     , (SELECT COUNT(1) FROM " + $ExcelOwner + "." + $ExcelTable + " ) AS 타겟검증값"
             $imple_sel += "     , (SELECT COUNT(1) FROM `"bludbev`".`"ext_mds`".`"" + $Excelasistable + "`" WHERE `"" + $chgKey + "`" BETWEEN '{BASE_DT_FROM}' AND '{BASE_DT_TO}' ) AS 원천검증값 "
             $imple_sel += " ;"
             $imple_sel += "####SQL"
         } elseif ($chgYN -eq 'Y') { #$statement 를 만드는 로직 추가
             $LC_sel_statement += "SELECT '{BASE_DT_TO}' AS 기준일자"
             $LC_sel_statement += "     , '" + $ExcelTable + "' AS 테이블ID"
             $LC_sel_statement += "     , 'COUNT(1)' AS 컬럼ID" 
             $LC_sel_statement += "     , CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' AS ETL작업일시"
             $LC_sel_statement += "     , 'UT_" + $ExcelTable + "_01' AS 테스트ID"
             $LC_sel_statement += "     , '" + $ExcelTableNm + "' AS 테이블명"
             $LC_sel_statement += "     , '모수검증' AS 컬럼명"
             $LC_sel_statement += "     , '" + $Excelasistable +"' AS 원천테이블ID"
             $LC_sel_statement += "     , '" + $TempTableNm + "' AS 원천테이블명"
             $LC_sel_statement += "     , 'COUNT(1)' AS 원천컬럼ID"
             $LC_sel_statement += "     , '모수검증' AS 원천컬럼명"
             if ($Excelchgyn -eq 'Y') {
                 $LC_sel_statement += "     , (SELECT COUNT(1) FROM " + $ExcelOwner + "." + $ExcelTable + " WHERE TO_CHAR(" + $Excelchgkey + ",'YYYYMMDD') >= '{BASE_DT_TO}' ) AS 타겟검증값"
             } elseif($Excelchgyn -eq 'S') {
                 $LC_sel_statement += "     , (SELECT COUNT(1) FROM " + $ExcelOwner + "." + $ExcelTable + " WHERE CRI_YMD = '{BASE_DT_TO}' ) AS 타겟검증값"
             } else {
                 $LC_sel_statement += "     , (SELECT COUNT(1) FROM " + $ExcelOwner + "." + $ExcelTable + " WHERE SUBSTRING(ETL_WRK_DTM,1,8) >= '{BASE_DT_TO}') AS 타겟검증값"
             }
             $LC_sel_statement += "     , (SELECT COUNT(1) FROM `"bludbev`".`"ext_mds`".`"" + $Excelasistable + "`") AS 원천검증값 "
             if ($i -eq $rowcount) {
                 $LC_sel_statement += " ;"
             } else {
                 $LC_sel_statement += " UNION ALL"
             }
         } else {
             $LI_sel_statement += "SELECT '{BASE_DT_TO}' AS 기준일자"
             $LI_sel_statement += "     , '" + $ExcelTable + "' AS 테이블ID"
             $LI_sel_statement += "     , 'COUNT(1)' AS 컬럼ID" 
             $LI_sel_statement += "     , CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' AS ETL작업일시"
             $LI_sel_statement += "     , 'UT_" + $ExcelTable + "_01' AS 테스트ID"
             $LI_sel_statement += "     , '" + $ExcelTableNm + "' AS 테이블명"
             $LI_sel_statement += "     , '모수검증' AS 컬럼명"
             $LI_sel_statement += "     , '" + $Excelasistable +"' AS 원천테이블ID"
             $LI_sel_statement += "     , '" + $TempTableNm + "' AS 원천테이블명"
             $LI_sel_statement += "     , 'COUNT(1)' AS 원천컬럼ID"
             $LI_sel_statement += "     , '모수검증' AS 원천컬럼명"
             if ($Excelchgyn -eq 'S') {
                 $LI_sel_statement += "     , (SELECT COUNT(1) FROM " + $ExcelOwner + "." + $ExcelTable + " WHERE CRI_YMD = '{BASE_DT_TO}' ) AS 타겟검증값"
             } else {
                 $LI_sel_statement += "     , (SELECT COUNT(1) FROM " + $ExcelOwner + "." + $ExcelTable + ") AS 타겟검증값"
             }
             $LI_sel_statement += "     , (SELECT COUNT(1) FROM `"bludbev`".`"ext_mds`".`"" + $Excelasistable + "`") AS 원천검증값 "
             if ($i -eq $rowcount) {
                 $LI_sel_statement += " ;"
             } else {
                 $LI_sel_statement += " UNION ALL"
             }
         }
    
     }
         $statement += "/*"
         $statement += "####################################################################################################"
         $statement += "1.  작성자       : 윤혁준"
         $statement += "2.  타겟테이블명 : SCM_BAT_WRK_VFCT_L (SCM_BATCH작업검증_로그)"
         $statement += "3.  작성일자     : 2023.08.08"
         $statement += "4.  프로시져명   : LCD_SCM_BAT_WRK_VFCT_L_01"
         $statement += "5.  모집단       : SOR 영역 적재 건수 검증 프로그램"
         $statement += "7.  특이사항     : 없음"
         $statement += "8.  집계시작시점 :"
         $statement += "9.  집계기준     : 일"
         $statement += "10. 변경내역     :"
         $statement += "####################################################################################################"
         $statement += "*/"
         $statement += "/**************************************************************************************************/"
         $statement += "/*  P_BAT_STG_NM : SET                                                                            */"
         $statement += "/**************************************************************************************************/"
         $statement += "SET enable_case_sensitive_identifier TO TRUE;"
         $statement += "####SQL"
         $statement += "/**************************************************************************************************/"
         $statement += "/* P_BAT_STG_NM : Target Delete                                                                   */"
         $statement += "/**************************************************************************************************/"
         $statement += "DELETE"
         $statement += "  FROM ANSOR.SCM_BAT_WRK_VFCT_L                                                                     /* SCM_BATCH작업검증_로그 */"
         $statement += " WHERE CRI_YMD = '{BASE_DT_TO}'                                                                     /* 기준일자 */"
         $statement += "   AND TLE_ID LIKE 'S%'                                                                             /* 테이블ID */"
         $statement += ";"
         $statement += "####SQL"
         $statement += "/**************************************************************************************************/"
         $statement += "/* P_BAT_STG_NM : Target Insert                                                                   */"
         $statement += "/**************************************************************************************************/"
         if ($chgYN -eq 'Y') {
             $statement += "INSERT INTO ANSOR.SCM_BAT_WRK_VFCT_L"
             $statement += "     ("
             $statement += "       CRI_YMD"
             $statement += "     , TLE_ID"
             $statement += "     , CLMN_ID"
             $statement += "     , ETL_WRK_DTM"
             $statement += "     , TEST_ID"
             $statement += "     , TLE_NM"
             $statement += "     , CLMN_NM"
             $statement += "     , SRCE_TLE_ID"
             $statement += "     , SRCE_TLE_NM"
             $statement += "     , SRCE_CLMN_ID"
             $statement += "     , SRCE_CLMN_NM"
             $statement += "     , TGT_VFCT_VAL"
             $statement += "     , SRCE_VFCT_VAL"
             $statement += "     )"
             $statement += $LC_sel_statement
         } else {
             $statement += "INSERT INTO ANSOR.SCM_BAT_WRK_VFCT_L"
             $statement += "     ("
             $statement += "       CRI_YMD"
             $statement += "     , TLE_ID"
             $statement += "     , CLMN_ID"
             $statement += "     , ETL_WRK_DTM"
             $statement += "     , TEST_ID"
             $statement += "     , TLE_NM"
             $statement += "     , CLMN_NM"
             $statement += "     , SRCE_TLE_ID"
             $statement += "     , SRCE_TLE_NM"
             $statement += "     , SRCE_CLMN_ID"
             $statement += "     , SRCE_CLMN_NM"
             $statement += "     , TGT_VFCT_VAL"
             $statement += "     , SRCE_VFCT_VAL"
             $statement += "     )"
             $statement += $LI_sel_statement
         }
         [System.IO.File]::WriteAllLines("C:\Users\Owner\Documents\source\검증\LCD_SCM_BAT_WRK_VFCT_L_01.sql",$statement,$utf8NoBomEncoding)
         $impl_sel_stat += "/*"    
         $impl_sel_stat += "####################################################################################################"
         $impl_sel_stat += "1.  작성자       : 윤혁준"
         $impl_sel_stat += "2.  타겟테이블명 : SCM_BAT_WRK_VFCT_L (SCM_BATCH작업검증_로그)"
         $impl_sel_stat += "3.  작성일자     : 2023.08.08"
         $impl_sel_stat += "4.  프로시져명   : LCD_SCM_BAT_WRK_VFCT_L_02"
         $impl_sel_stat += "5.  모집단       : SOR 영역 적재 건수 검증 프로그램"
         $impl_sel_stat += "7.  특이사항     : 없음"
         $impl_sel_stat += "8.  집계시작시점 :"
         $impl_sel_stat += "9.  집계기준     : 일"
         $impl_sel_stat += "10. 변경내역     :"
         $impl_sel_stat += "####################################################################################################"
         $impl_sel_stat += "*/"
         $impl_sel_stat += "/**************************************************************************************************/"
         $impl_sel_stat += "/*  P_BAT_STG_NM : SET                                                                            */"
         $impl_sel_stat += "/**************************************************************************************************/"
         $impl_sel_stat += "SET enable_case_sensitive_identifier TO TRUE;"
         $impl_sel_stat += "####SQL"
         $impl_sel_stat += $imple_sel
         [System.IO.File]::WriteAllLines("C:\Users\Owner\Documents\source\검증\LCD_SCM_BAT_WRK_VFCT_L_02.sql",$impl_sel_stat,$utf8NoBomEncoding)

             <#$redconnection = New-Object System.Data.Odbc.OdbcConnection($redconnectionString)
             $redconnection.Open()

    
             $command = New-Object System.Data.Odbc.OdbcCommand($statement, $redconnection)
             $command.CommandText = $statement
             $result = $command.ExecuteReader()#>

     Write-Host "=================================================================================================="
     Write-Host "=============================================작업완료============================================="
     Write-Host "=================================================================================================="

} catch {
     Write-Error ("Database Exception:{0}{1}" -f ` $con.ConnectionString, $_.Exception.ToString())
} finally{
     if ($redconnection.State -eq 'Open') {
        $redconnection.close()
     }
     $ExcelWorkBook.Close()
}