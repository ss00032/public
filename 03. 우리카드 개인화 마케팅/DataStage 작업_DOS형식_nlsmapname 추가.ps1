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
     $dsjoball=@()

     for ($i=2; $i -le $rowcount; $i++) {
         $ExcelTableNm = $ExcelWorkSheet.cells.Item($i, 4).value2
         $ExcelOwner = $ExcelWorkSheet.cells.Item($i, 5).value2
         $ExcelTable = $ExcelWorkSheet.cells.Item($i, 6).value2
         $ExcelTarTable = $ExcelWorkSheet.cells.Item($i, 8).value2
         $ExcelChgTable = $ExcelWorkSheet.cells.Item($i, 9).value2
         $ExcelParTable = $ExcelWorkSheet.cells.Item($i, 10).value2
         $ExcelParCnt = $ExcelWorkSheet.cells.Item($i, 11).value2
         $ExcelUserNm = $ExcelWorkSheet.cells.Item($i, 14).value2
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
                      `     , B.COMMENTS       AS TABLE_COMMENTS
                      `     , A.COLUMN_NAME
                      `     , nvl(C.COMMENTS,'없음')       AS COLUMN_COMMENTS
                      `     , A.DATA_TYPE
                      `     , A.DATA_LENGTH
                      `     , nvl(A.DATA_PRECISION,'-999')
                      `     , nvl(A.DATA_SCALE,'0')
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
         $nullchk=@()
         $colArr=@()
         $colArr2=@()
         $colS1=@()
         $colT1=@()
         $colSDef=@()
         $colTDef=@()

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
          $nullable = $result.GetString(9)
          $col_id = $result.GetValue(10)
          #$deflength = $result.GetValue(11)
          $datadef = $result.GetString(12)
          $pkyn = $result.GetString(13)

          #PK 설정 (keyposition) Y=PK=1
          if ($pkyn -eq 'Y') {
             $pkyn = '1'
          } else {
             $pkyn = '0'
          }

          #null 설정 (nullable) N=NOT NULL=0
          if ($nullable -eq 'N') {
            $nullchk = 0
          } else {
            $nullchk = 1
          }
          $tobeCol_id = $col_id +1
          $tobeCol_id1 = $col_id +2
          $tobeCol_id2 = $col_id +3

          # 제외 컬럼 (삭제된 컬럼) 
          # 원천 테이블에는 존재하지만 타겟에서 삭제
          # 원천 테이블 레이아웃을 읽어오므로 방어로직 필요
          if ($column -eq 'RAND_VAL' -or $column -eq 'SMS_SEND_TELNO') {
             continue
          }

          # 제외 컬럼 (삭제된 컬럼) 
          # 원천 테이블에는 존재하지만 타겟에서 삭제
          # 원천 테이블 레이아웃을 읽어오므로 방어로직 필요
          if ($ExcelTarTable -eq 'WCDIFS52TH' -and ($column -eq 'CUST_ID_INFO1' -or $column -eq 'CUST_ID_INFO2' -or $column -eq 'CUST_ID_INFO3' -or $column -eq 'INFO_FLD1' -or $column -eq 'INFO_FLD2' -or $column -eq 'INFO_FLD3' -or $column -eq 'INFO_FLD4' -or $column -eq 'INFO_FLD5')) {
             continue
          }

          #type 설정 (VARCHAR2 = 12, NUMBER = Decimal = 3, DATE = Date = 9
          $sqltype = 12
          if ($datatype -eq 'VARCHAR2') {
             $datatype = 'VarChar'
             $sqltype = 12
          }

          if ($datatype -eq 'NUMBER') {
             $datatype = 'Decimal'
             $sqltype = 3
          }

          if ($datatype -eq 'DATE') {
             $datatype = 'Date'
             $sqltype = 9
          }

          #원천 테이블에 동일명칭 컬럼 수정
          if ($column -eq 'LOAD_DH') {
             $column = 'MKT_LOAD_DH'
          }

if ($column -ne 'WFG_CD') {
  #NUMBER타입 소수점 존재 여부 확인 후 값 셋팅
  if ($datatype -eq 'Decimal' -and $dataprecision -ne '-999') {
$colArr += "   BEGIN DSSUBRECORD
`      Name `"$column`"
`      Description `"$columnnm`"
`      SqlType `"$sqltype`"
`      Precision `"$dataprecision`"
`      Scale `"$datascale`"
`      Nullable `"$nullchk`"
`      KeyPosition `"$pkyn`"
`      DisplaySize `"0`"
`      Group `"0`"
`      SortKey `"0`"
`      SortType `"0`"
`      TableDef `"Database\\IFIN\\CDOWN.$ExcelTarTable`"
`      AllowCRLF `"0`"
`      LevelNo `"0`"
`      Occurs `"0`"
`      PadNulls `"0`"
`      SignOption `"0`"
`      SortingOrder `"0`"
`      ArrayHandling `"0`"
`      SyncIndicator `"0`"
`      PadChar `"`"
`      ColumnReference `"$column`"
`      ExtendedPrecision `"0`"
`      TaggedSubrec `"0`"
`      OccursVarying `"0`"
`      PKeyIsCaseless `"0`"
`      SCDPurpose `"0`"
`   END DSSUBRECORD
` "
#Date 타입 설정
} elseif ($datatype -eq 'Date') {
$colArr += "   BEGIN DSSUBRECORD
`      Name `"$column`"
`      Description `"$columnnm`"
`      SqlType `"$sqltype`"
`      Precision `"`"
`      Scale `"$datascale`"
`      Nullable `"$nullchk`"
`      KeyPosition `"$pkyn`"
`      DisplaySize `"0`"
`      Group `"0`"
`      SortKey `"0`"
`      SortType `"0`"
`      TableDef `"Database\\IFIN\\CDOWN.$ExcelTarTable`"
`      AllowCRLF `"0`"
`      LevelNo `"0`"
`      Occurs `"0`"
`      PadNulls `"0`"
`      SignOption `"0`"
`      SortingOrder `"0`"
`      ArrayHandling `"0`"
`      SyncIndicator `"0`"
`      PadChar `"`"
`      ColumnReference `"$column`"
`      ExtendedPrecision `"0`"
`      TaggedSubrec `"0`"
`      OccursVarying `"0`"
`      PKeyIsCaseless `"0`"
`      SCDPurpose `"0`"
`   END DSSUBRECORD
` "
} else {
$colArr += "   BEGIN DSSUBRECORD
`      Name `"$column`"
`      Description `"$columnnm`"
`      SqlType `"$sqltype`"
`      Precision `"$datalength`"
`      Scale `"$datascale`"
`      Nullable `"$nullchk`"
`      KeyPosition `"$pkyn`"
`      DisplaySize `"0`"
`      Group `"0`"
`      SortKey `"0`"
`      SortType `"0`"
`      TableDef `"Database\\IFIN\\CDOWN.$ExcelTarTable`"
`      AllowCRLF `"0`"
`      LevelNo `"0`"
`      Occurs `"0`"
`      PadNulls `"0`"
`      SignOption `"0`"
`      SortingOrder `"0`"
`      ArrayHandling `"0`"
`      SyncIndicator `"0`"
`      PadChar `"`"
`      ColumnReference `"$column`"
`      ExtendedPrecision `"0`"
`      TaggedSubrec `"0`"
`      OccursVarying `"0`"
`      PKeyIsCaseless `"0`"
`      SCDPurpose `"0`"
`   END DSSUBRECORD
` "
}
          }
if ($column -ne 'WFG_CD') {
  if ($datatype -eq 'Decimal' -and $dataprecision -ne '-999') {
#DataStage 컬럼리스트 2개 필요.
$colArr2 += "   BEGIN DSSUBRECORD
`      Name `"$column`"
`      Description `"$columnnm`"
`      SqlType `"$sqltype`"
`      Precision `"$dataprecision`"
`      Scale `"$datascale`"
`      Nullable `"$nullchk`"
`      KeyPosition `"$pkyn`"
`      DisplaySize `"0`"
`      Derivation `"S_01.$column`"
`      Group `"0`"
`      ParsedDerivation `"S_01.$column`"
`      SourceColumn `"S_01.$column`"
`      SortKey `"0`"
`      SortType `"0`"
`      TableDef `"Database\\IFIN\\CDOWN.$ExcelTarTable`"
`      AllowCRLF `"0`"
`      LevelNo `"0`"
`      Occurs `"0`"
`      PadNulls `"0`"
`      SignOption `"0`"
`      SortingOrder `"0`"
`      ArrayHandling `"0`"
`      SyncIndicator `"0`"
`      PadChar `"`"
`      ColumnReference `"$column`"
`      ExtendedPrecision `"0`"
`      TaggedSubrec `"0`"
`      OccursVarying `"0`"
`      PKeyIsCaseless `"0`"
`      SCDPurpose `"0`"
`   END DSSUBRECORD
` "
} elseif ($datatype -eq 'Date') {
$colArr2 += "   BEGIN DSSUBRECORD
`      Name `"$column`"
`      Description `"$columnnm`"
`      SqlType `"$sqltype`"
`      Precision `"`"
`      Scale `"$datascale`"
`      Nullable `"$nullchk`"
`      KeyPosition `"$pkyn`"
`      DisplaySize `"0`"
`      Derivation `"S_01.$column`"
`      Group `"0`"
`      ParsedDerivation `"S_01.$column`"
`      SourceColumn `"S_01.$column`"
`      SortKey `"0`"
`      SortType `"0`"
`      TableDef `"Database\\IFIN\\CDOWN.$ExcelTarTable`"
`      AllowCRLF `"0`"
`      LevelNo `"0`"
`      Occurs `"0`"
`      PadNulls `"0`"
`      SignOption `"0`"
`      SortingOrder `"0`"
`      ArrayHandling `"0`"
`      SyncIndicator `"0`"
`      PadChar `"`"
`      ColumnReference `"$column`"
`      ExtendedPrecision `"0`"
`      TaggedSubrec `"0`"
`      OccursVarying `"0`"
`      PKeyIsCaseless `"0`"
`      SCDPurpose `"0`"
`   END DSSUBRECORD
` "
} else {
$colArr2 += "   BEGIN DSSUBRECORD
`      Name `"$column`"
`      Description `"$columnnm`"
`      SqlType `"$sqltype`"
`      Precision `"$datalength`"
`      Scale `"$datascale`"
`      Nullable `"$nullchk`"
`      KeyPosition `"$pkyn`"
`      DisplaySize `"0`"
`      Derivation `"S_01.$column`"
`      Group `"0`"
`      ParsedDerivation `"S_01.$column`"
`      SourceColumn `"S_01.$column`"
`      SortKey `"0`"
`      SortType `"0`"
`      TableDef `"Database\\IFIN\\CDOWN.$ExcelTarTable`"
`      AllowCRLF `"0`"
`      LevelNo `"0`"
`      Occurs `"0`"
`      PadNulls `"0`"
`      SignOption `"0`"
`      SortingOrder `"0`"
`      ArrayHandling `"0`"
`      SyncIndicator `"0`"
`      PadChar `"`"
`      ColumnReference `"$column`"
`      ExtendedPrecision `"0`"
`      TaggedSubrec `"0`"
`      OccursVarying `"0`"
`      PKeyIsCaseless `"0`"
`      SCDPurpose `"0`"
`   END DSSUBRECORD
` "
}
          }
      }

#변경적재가 아닌경우 초기적재형태로 생성
if($ExcelChgTable -eq 'X') {

$loadman = "TRUNCATE/INSERT"

$dsjob  = "BEGIN HEADER
`   CharacterSet `"CP949`"
`   ExportingTool `"IBM InfoSphere DataStage Export`"
`   ToolVersion `"8`"
`   ServerName `"CDIDBT01`"
`   ToolInstanceID `"CDW_DEV`"
`   MDISVersion `"1.0`"
`   Date `"`"
`   Time `"`"
`   ServerVersion `"8.5`"
`END HEADER
`BEGIN DSJOB
`   Identifier `"$ExcelTarTable`_J`"
`   DateModified `"`"
`   TimeModified `"`"
`   BEGIN DSRECORD
`      Identifier `"ROOT`"
`      OLEType `"CJobDefn`"
`      Readonly `"0`"
`      Name `"$ExcelTarTable`_J`"
`      Description `"$ExcelTableNm`"
`      NextID `"7`"
`      Container `"V0`"
`      FullDescription `"1.소스     : $ExcelTarTable`_`$yyyymmdd.dat\(D)\(A)2.타겟     : $ExcelTarTable`\(D)\(A)3.작성자   : $ExcelUserNm`\(D)\(A)4.적재방식 : $loadman`\(D)\(A)5.이력     : 2022.09.01 최초 생성\(D)\(A)6.설명     : $ExcelTableNm (변경적재)`"
`      JobVersion `"`"
`      BeforeSubr `"DSU.RsDwComn\\LB`"
`      AfterSubr `"DSU.RsDwComn\\LA`"
`      ControlAfterSubr `"0`"
`      Parameters `"CParameters`"
`      BEGIN DSSUBRECORD
`         Name `"ParaETL`"
`         Prompt `"ParaETL 매개변수`"
`         Default `"(As pre-defined)`"
`         HelpTxt `"카드DW 접속정보`"
`         ParamType `"13`"
`         ParamLength `"0`"
`         ParamScale `"0`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"`$yyyymmdd`"
`         Prompt `"yyyymmdd`"
`         ParamType `"0`"
`         ParamLength `"0`"
`         ParamScale `"0`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"`$HIRK_SEQ_INF`"
`         Prompt `"HIRK_SEQ_INF`"
`         ParamType `"0`"
`         ParamLength `"0`"
`         ParamScale `"0`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"`$POSI_SEQ_INF`"
`         Prompt `"POSI_SEQ_INF`"
`         ParamType `"0`"
`         ParamLength `"0`"
`         ParamScale `"0`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"`$IFIN_TMP_DIR`"
`         Prompt `"IFIN_TMP_DIR`"
`         Default `"`$PROJDEF`"
`         ParamType `"0`"
`         ParamLength `"0`"
`         ParamScale `"0`"
`      END DSSUBRECORD
`      MetaBag `"CMetaProperty`"
`      BEGIN DSSUBRECORD
`         Owner `"APT`"
`         Name `"AdvancedRuntimeOptions`"
`         Value `"#DSProjectARTOptions#`"
`      END DSSUBRECORD
`      NULLIndicatorPosition `"0`"
`      IsTemplate `"0`"
`      NLSLocale `",,,,`"
`      JobType `"0`"
`      Category `"\\Jobs\\IF_IN\\S.개인화마케팅`"
`      CenturyBreakYear `"30`"
`      NextAliasID `"2`"
`      ParameterFileDDName `"DD00001`"
`      ReservedWordCheck `"1`"
`      TransactionSize `"0`"
`      ValidationStatus `"0`"
`      Uploadable `"0`"
`      PgmCustomizationFlag `"0`"
`      JobReportFlag `"0`"
`      AllowMultipleInvocations `"0`"
`      Act2ActOverideDefaults `"0`"
`      Act2ActEnableRowBuffer `"0`"
`      Act2ActUseIPC `"0`"
`      Act2ActBufferSize `"0`"
`      Act2ActIPCTimeout `"0`"
`      ExpressionSemanticCheckFlag `"0`"
`      TraceOption `"0`"
`      EnableCacheSharing `"0`"
`      RuntimeColumnPropagation `"0`"
`      RelStagesInJobStatus `"-1`"
`      WebServiceEnabled `"0`"
`      MFProcessMetaData `"0`"
`      MFProcessMetaDataXMLFileExchangeMethod `"0`"
`      IMSProgType `"0`"
`      CopyLibPrefix `"ARDT`"
`      RecordPerformanceResults `"0`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V0`"
`      OLEType `"CContainerView`"
`      Readonly `"0`"
`      Name `"\(C791)\(C5C5)`"
`      NextID `"1`"
`      IsTopLevel `"0`"
`      StageList `"V6S0|V6S1|V6S2|V6A3`"
`      StageXPos `"`" [수정필요]
`      StageYPos `"`" [수정필요]
`      StageTypes `"CSeqFileStage|CTransformerStage|CCustomStage.ORAOCI9|ID_PALETTEJOBANNOTATION`"
`      NextStageID `"0`"
`      SnapToGrid `"1`"
`      GridLines `"0`"
`      ZoomValue `"100`"
`      StageXSize `"`"
`      StageYSize `"`" [수정필요]
`      ContainerViewSizing `"`" [수정필요]
`      StageNames `"S_$ExcelTarTable`_J|TR01|T_$ExcelTarTable`| `"
`      StageTypeIDs `"CSeqFileStage|CTransformerStage|ORAOCI9| `"
`      LinkNames `"S_01|T_01| | `"
`      LinkHasMetaDatas `"True|True| | `"
`      LinkTypes `"1|1| | `"
`      LinkNamePositionXs `"`" [수정필요]
`      LinkNamePositionYs `"`" [수정필요]
`      TargetStageIDs `"V6S1|V6S2| | `"
`      SourceStageEffectiveExecutionModes `"0|0| | `"
`      SourceStageRuntimeExecutionModes `"0|0| | `"
`      TargetStageEffectiveExecutionModes `"0|0| | `"
`      TargetStageRuntimeExecutionModes `"0|0| | `"
`      LinkIsSingleOperatorLookup `"False|False| | `"
`      LinkIsSortSequential `"False|False| | `"
`      LinkSortMode `"0|0| | `"
`      LinkPartColMode `"0|0| | `"
`      LinkSourcePinIDs `"V6S0P1|V6S1P2| | `"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6A3`"
`      OLEType `"CAnnotation`"
`      Readonly `"0`"
`      Name `"V6A3`"
`      NextID `"0`"
`      AnnotationType `"2`"
`      TextFont `"\(AD74)\(B9BC)\(CCB4)\\10\\0\\0\\0\\400\\129`"
`      TextHorizontalJustification `"0`"
`      TextVerticalJustification `"0`"
`      TextColor `"0`"
`      BackgroundColor `"`"
`      BackgroundTransparent `"0`"
`      BorderVisible `"1`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S0`"
`      OLEType `"CSeqFileStage`"
`      Readonly `"0`"
`      Name `"S_$ExcelTarTable`_J`"
`      NextID `"2`"
`      OutputPins `"V6S0P1`"
`      UnixFormat `"1`"
`      NLSMapName `"NONE`"
`      PipeStage `"0`"
`      UnicodeBOM `"1`"
`      UnicodeSwapped `"1`"
`      AllowColumnMapping `"1`"
`      WithFilter `"0`"
`      StageType `"CSeqFileStage`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S0P1`"
`      OLEType `"CSeqOutput`"
`      Readonly `"0`"
`      Name `"S_01`"
`      Partner `"V6S1|V6S1P1`"
`      FileName `"#`$IFIN_TMP_DIR#/$ExcelTarTable`_J.ifin_tmp`"
`      ColDelim `"|`"
`      QuoteChar `"000`"
`      ColHeaders `"0`"
`      FixedWidth `"0`"
`      ColSpace `"0`"
`      EnforceMetaData `"0`"
`      Readtimeout `"0`"
`      Columns `"COutputColumn`"
` $colArr
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S1`"
`      OLEType `"CTransformerStage`"
`      Readonly `"0`"
`      Name `"TR01`"
`      NextID `"3`"
`      InputPins `"V6S1P1`"
`      OutputPins `"V6S1P2`"
`      ValidationStatus `"0`"
`      StageType `"CTransformerStage`"
`      BlockSize `"0`"
`      StageVars `"CStageVar`"
`      BEGIN DSSUBRECORD
`         Name `"sJobNm`"
`         InitialValue `"DSJobName`"
`         SqlType `"3`"
`         Precision `"18`"
`         ColScale `"4`"
`         ExtendedPrecision `"0`"
`      END DSSUBRECORD
`      StageVarsMinimised `"0`"
`      LoopVarsMaximised `"0`"
`      MaxLoopIterations `"0`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S1P1`"
`      OLEType `"CTrxInput`"
`      Readonly `"0`"
`      Name `"S_01`"
`      Partner `"V6S0|V6S0P1`"
`      LinkType `"1`"
`      MultiRow `"0`"
`      LinkMinimised `"0`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S1P2`"
`      OLEType `"CTrxOutput`"
`      Readonly `"0`"
`      Name `"T_01`"
`      Partner `"V6S2|V6S2P1`"
`      Reject `"0`"
`      ErrorPin `"0`"
`      RowLimit `"0`"
`      Columns `"COutputColumn`"
`      BEGIN DSSUBRECORD
`         Name `"WFG_CD`"
`         Description `"그룹내기관코드`"
`         SqlType `"12`"
`         Precision `"2`"
`         Scale `"0`"
`         Nullable `"0`"
`         KeyPosition `"1`"
`         DisplaySize `"0`"
`         Derivation `"\`"01\`"`"
`         Group `"0`"
`         ParsedDerivation `"\`"01\`"`"
`         SortKey `"0`"
`         SortType `"0`"
`         TableDef `"Database\\IFIN\\CDOWN.$ExcelTarTable`"
`         AllowCRLF `"0`"
`         LevelNo `"0`"
`         Occurs `"0`"
`         PadNulls `"0`"
`         SignOption `"0`"
`         SortingOrder `"0`"
`         ArrayHandling `"0`"
`         SyncIndicator `"0`"
`         PadChar `"`"
`         ColumnReference `"WFG_CD`"
`         ExtendedPrecision `"0`"
`         TaggedSubrec `"0`"
`         OccursVarying `"0`"
`         PKeyIsCaseless `"0`"
`         SCDPurpose `"0`"
`      END DSSUBRECORD
` $colArr2
`      BEGIN DSSUBRECORD
`         Name `"LOAD_PGM_ID`"
`         Description `"적재프로그램ID`"
`         SqlType `"12`"
`         Precision `"16`"
`         Scale `"0`"
`         Nullable `"0`"
`         KeyPosition `"0`"
`         DisplaySize `"0`"
`         Derivation `"sJobNm`"
`         Group `"0`"
`         ParsedDerivation `"sJobNm`"
`         SortKey `"0`"
`         SortType `"0`"
`         TableDef `"Database\\IFIN\\CDOWN.$ExcelTarTable`"
`         AllowCRLF `"0`"
`         LevelNo `"0`"
`         Occurs `"0`"
`         PadNulls `"0`"
`         SignOption `"0`"
`         SortingOrder `"0`"
`         StageVars `"sJobNm`"
`         ArrayHandling `"0`"
`         SyncIndicator `"0`"
`         PadChar `"`"
`         ColumnReference `"LOAD_PGM_ID`"
`         ExtendedPrecision `"0`"
`         TaggedSubrec `"0`"
`         OccursVarying `"0`"
`         PKeyIsCaseless `"0`"
`         SCDPurpose `"0`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"LOAD_DH`"
`         Description `"적재일시`"
`         SqlType `"12`"
`         Precision `"14`"
`         Scale `"0`"
`         Nullable `"0`"
`         KeyPosition `"0`"
`         DisplaySize `"0`"
`         Derivation `"RsLoadDH(0)`"
`         Group `"0`"
`         ParsedDerivation `"RsLoadDH(0)`"
`         Transform `"RsLoadDH`"
`         SortKey `"0`"
`         SortType `"0`"
`         TableDef `"Database\\IFIN\\CDOWN.WCDIFS15TH`"
`         AllowCRLF `"0`"
`         LevelNo `"0`"
`         Occurs `"0`"
`         PadNulls `"0`"
`         SignOption `"0`"
`         SortingOrder `"0`"
`         ArrayHandling `"0`"
`         SyncIndicator `"0`"
`         PadChar `"`"
`         ColumnReference `"LOAD_DH`"
`         ExtendedPrecision `"0`"
`         TaggedSubrec `"0`"
`         OccursVarying `"0`"
`         PKeyIsCaseless `"0`"
`         SCDPurpose `"0`"
`      END DSSUBRECORD
`      LeftTextPos `"375`"
`      TopTextPos `"192`"
`      LinkMinimised `"0`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S2`"
`      OLEType `"CCustomStage`"
`      Readonly `"0`"
`      Name `"T_$ExcelTarTable`"
`      NextID `"2`"
`      InputPins `"V6S2P1`"
`      StageType `"ORAOCI9`"
`      NLSMapName `"NONE`"
`      AllowColumnMapping `"0`"
`      Properties `"CCustomProperty`"
`      BEGIN DSSUBRECORD
`         Name `"DATABASE`"
`         Value `"#ParaETL.`$ETL_SID#`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"USERID`"
`         Value `"#ParaETL.`$ETL_UID#`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"PASSWORD`"
`         Value `"HD:@IJV@L93?0G7IL;JL0K87F5LIE@UD3U;<k?Kd?0QG1P7HH=0dG5`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"ISOPS`"
`         Value `"NO`"
`      END DSSUBRECORD
`      NextRecordID `"0`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S2P1`"
`      OLEType `"CCustomInput`"
`      Readonly `"0`"
`      Name `"T_01`"
`      Partner `"V6S1|V6S1P2`"
`      LinkType `"1`"
`      ConditionNotMet `"fail`"
`      LookupFail `"fail`"
`      Properties `"CCustomProperty`"
`      BEGIN DSSUBRECORD
`         Name `"TABLE`"
`         Value `"CDOWN.$ExcelTarTable`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"ISCASESENS`"
`         Value `"NO`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"GENSQL`"
`         Value `"Yes`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"USERSQL`"
`         Value `"NO`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"SQLBUILDERSQL`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"FULLYGENSQL`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"Pre42BINDING`"
`         Value `"Yes`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"ARRAYSIZE`"
`         Value `"10000`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"TRANSSIZE`"
`         Value `"20000`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"CREATETABLE`"
`         Value `"No`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"GENCREATEDDL`"
`         Value `"Yes`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"USERCREATEDDL`"
`         Value `"NO`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"DROPTABLE`"
`         Value `"No`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"GENDROPDDL`"
`         Value `"Yes`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"USERDROPDDL`"
`         Value `"DROP TABLE CDOWN.$ExcelTarTable`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"TABLESPACE`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"STORAGERULE`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"DODELETE`"
`         Value `"Trunc`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"DOUPDATE`"
`         Value `"Insert rows`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"TREATWARNASFATAL`"
`         Value `"NO`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"TX_ISO_LEVELI`"
`         Value `"Read Committed`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"BEFORESQL`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"CONTBEFORESQL`"
`         Value `"YES`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"AFTERSQL`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"CONTAFTERSQL`"
`         Value `"YES`"
`      END DSSUBRECORD
`      TransactionSize `"0`"
`      TXNBehaviour `"40`"
`      EnableTxGroup `"0`"
`      LinkMinimised `"0`"
`   END DSRECORD
`END DSJOB"
$dsjoball += $dsjob
$dsjob > C:\Users\woori\Documents\99.source\DSJOB\$ExcelTarTable"_J".dsx

#변경적재인 경우 초기적재 프로그램, 변경적재 프로그램 두개 다 생성
        } else {
            
            $loadman = "TRUNCATE/INSERT"
            $loadmanTemp = "DELETE/INSERT"

$dsjob  = "BEGIN HEADER
`   CharacterSet `"CP949`"
`   ExportingTool `"IBM InfoSphere DataStage Export`"
`   ToolVersion `"8`"
`   ServerName `"CDIDBT01`"
`   ToolInstanceID `"CDW_DEV`"
`   MDISVersion `"1.0`"
`   Date `"`"
`   Time `"`"
`   ServerVersion `"8.5`"
`END HEADER
`BEGIN DSJOB
`   Identifier `"$ExcelTarTable`_J`"
`   DateModified `"`"
`   TimeModified `"`"
`   BEGIN DSRECORD
`      Identifier `"ROOT`"
`      OLEType `"CJobDefn`"
`      Readonly `"0`"
`      Name `"$ExcelTarTable`_J`"
`      Description `"$ExcelTableNm`"
`      NextID `"7`"
`      Container `"V0`"
`      FullDescription `"1.소스     : $ExcelTarTable`_`$yyyymmdd.dat\(D)\(A)2.타겟     : $ExcelTarTable`\(D)\(A)3.작성자   : $ExcelUserNm`\(D)\(A)4.적재방식 : $loadman`\(D)\(A)5.이력     : 2022.09.01 최초 생성\(D)\(A)6.설명     : $ExcelTableNm (변경적재)`"
`      JobVersion `"`"
`      BeforeSubr `"DSU.RsDwComn\\LB`"
`      AfterSubr `"DSU.RsDwComn\\LA`"
`      ControlAfterSubr `"0`"
`      Parameters `"CParameters`"
`      BEGIN DSSUBRECORD
`         Name `"ParaETL`"
`         Prompt `"ParaETL 매개변수`"
`         Default `"(As pre-defined)`"
`         HelpTxt `"카드DW 접속정보`"
`         ParamType `"13`"
`         ParamLength `"0`"
`         ParamScale `"0`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"`$yyyymmdd`"
`         Prompt `"yyyymmdd`"
`         ParamType `"0`"
`         ParamLength `"0`"
`         ParamScale `"0`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"`$HIRK_SEQ_INF`"
`         Prompt `"HIRK_SEQ_INF`"
`         ParamType `"0`"
`         ParamLength `"0`"
`         ParamScale `"0`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"`$POSI_SEQ_INF`"
`         Prompt `"POSI_SEQ_INF`"
`         ParamType `"0`"
`         ParamLength `"0`"
`         ParamScale `"0`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"`$IFIN_TMP_DIR`"
`         Prompt `"IFIN_TMP_DIR`"
`         Default `"`$PROJDEF`"
`         ParamType `"0`"
`         ParamLength `"0`"
`         ParamScale `"0`"
`      END DSSUBRECORD
`      MetaBag `"CMetaProperty`"
`      BEGIN DSSUBRECORD
`         Owner `"APT`"
`         Name `"AdvancedRuntimeOptions`"
`         Value `"#DSProjectARTOptions#`"
`      END DSSUBRECORD
`      NULLIndicatorPosition `"0`"
`      IsTemplate `"0`"
`      NLSLocale `",,,,`"
`      JobType `"0`"
`      Category `"\\Jobs\\IF_IN\\S.개인화마케팅`"
`      CenturyBreakYear `"30`"
`      NextAliasID `"2`"
`      ParameterFileDDName `"DD00001`"
`      ReservedWordCheck `"1`"
`      TransactionSize `"0`"
`      ValidationStatus `"0`"
`      Uploadable `"0`"
`      PgmCustomizationFlag `"0`"
`      JobReportFlag `"0`"
`      AllowMultipleInvocations `"0`"
`      Act2ActOverideDefaults `"0`"
`      Act2ActEnableRowBuffer `"0`"
`      Act2ActUseIPC `"0`"
`      Act2ActBufferSize `"0`"
`      Act2ActIPCTimeout `"0`"
`      ExpressionSemanticCheckFlag `"0`"
`      TraceOption `"0`"
`      EnableCacheSharing `"0`"
`      RuntimeColumnPropagation `"0`"
`      RelStagesInJobStatus `"-1`"
`      WebServiceEnabled `"0`"
`      MFProcessMetaData `"0`"
`      MFProcessMetaDataXMLFileExchangeMethod `"0`"
`      IMSProgType `"0`"
`      CopyLibPrefix `"ARDT`"
`      RecordPerformanceResults `"0`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V0`"
`      OLEType `"CContainerView`"
`      Readonly `"0`"
`      Name `"\(C791)\(C5C5)`"
`      NextID `"1`"
`      IsTopLevel `"0`"
`      StageList `"V6S0|V6S1|V6S2|V6A3`"
`      StageXPos `"사팔공|이육사|사팔공|이오`"
`      StageYPos `"오일구이|일구이|일구이|일`"
`      StageTypes `"CSeqFileStage|CTransformerStage|CCustomStage.ORAOCI9|ID_PALETTEJOBANNOTATION`"
`      NextStageID `"0`"
`      SnapToGrid `"1`"
`      GridLines `"0`"
`      ZoomValue `"100`"
`      StageXSize `"사팔|사팔|사팔523`"
`      StageYSize `"사팔|사팔|사팔119`"
`      ContainerViewSizing `"`"
`      StageNames `"S_$ExcelTarTable`_J|TR01|T_$ExcelTarTable`| `"
`      StageTypeIDs `"CSeqFileStage|CTransformerStage|ORAOCI9| `"
`      LinkNames `"S_01|T_01| | `"
`      LinkHasMetaDatas `"True|True| | `"
`      LinkTypes `"1|1| | `"
`      LinkNamePositionXs `"`"
`      LinkNamePositionYs `"`"
`      TargetStageIDs `"V6S1|V6S2| | `"
`      SourceStageEffectiveExecutionModes `"0|0| | `"
`      SourceStageRuntimeExecutionModes `"0|0| | `"
`      TargetStageEffectiveExecutionModes `"0|0| | `"
`      TargetStageRuntimeExecutionModes `"0|0| | `"
`      LinkIsSingleOperatorLookup `"False|False| | `"
`      LinkIsSortSequential `"False|False| | `"
`      LinkSortMode `"0|0| | `"
`      LinkPartColMode `"0|0| | `"
`      LinkSourcePinIDs `"V6S0P1|V6S1P2| | `"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6A3`"
`      OLEType `"CAnnotation`"
`      Readonly `"0`"
`      Name `"V6A3`"
`      NextID `"0`"
`      AnnotationType `"2`"
`      TextFont `"\(AD74)\(B9BC)\(CCB4)\\10\\0\\0\\0\\400\\129`"
`      TextHorizontalJustification `"0`"
`      TextVerticalJustification `"0`"
`      TextColor `"0`"
`      BackgroundColor `"`"
`      BackgroundTransparent `"0`"
`      BorderVisible `"1`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S0`"
`      OLEType `"CSeqFileStage`"
`      Readonly `"0`"
`      Name `"S_$ExcelTarTable`_J`"
`      NextID `"2`"
`      OutputPins `"V6S0P1`"
`      UnixFormat `"1`"
`      NLSMapName `"NONE`"
`      PipeStage `"0`"
`      UnicodeBOM `"1`"
`      UnicodeSwapped `"1`"
`      AllowColumnMapping `"1`"
`      WithFilter `"0`"
`      StageType `"CSeqFileStage`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S0P1`"
`      OLEType `"CSeqOutput`"
`      Readonly `"0`"
`      Name `"S_01`"
`      Partner `"V6S1|V6S1P1`"
`      FileName `"#`$IFIN_TMP_DIR#/$ExcelTarTable`_J.ifin_tmp`"
`      ColDelim `"|`"
`      QuoteChar `"000`"
`      ColHeaders `"0`"
`      FixedWidth `"0`"
`      ColSpace `"0`"
`      EnforceMetaData `"0`"
`      Readtimeout `"0`"
`      Columns `"COutputColumn`"
` $colArr
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S1`"
`      OLEType `"CTransformerStage`"
`      Readonly `"0`"
`      Name `"TR01`"
`      NextID `"3`"
`      InputPins `"V6S1P1`"
`      OutputPins `"V6S1P2`"
`      ValidationStatus `"0`"
`      StageType `"CTransformerStage`"
`      BlockSize `"0`"
`      StageVars `"CStageVar`"
`      BEGIN DSSUBRECORD
`         Name `"sJobNm`"
`         InitialValue `"DSJobName`"
`         SqlType `"3`"
`         Precision `"18`"
`         ColScale `"4`"
`         ExtendedPrecision `"0`"
`      END DSSUBRECORD
`      StageVarsMinimised `"0`"
`      LoopVarsMaximised `"0`"
`      MaxLoopIterations `"0`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S1P1`"
`      OLEType `"CTrxInput`"
`      Readonly `"0`"
`      Name `"S_01`"
`      Partner `"V6S0|V6S0P1`"
`      LinkType `"1`"
`      MultiRow `"0`"
`      LinkMinimised `"0`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S1P2`"
`      OLEType `"CTrxOutput`"
`      Readonly `"0`"
`      Name `"T_01`"
`      Partner `"V6S2|V6S2P1`"
`      Reject `"0`"
`      ErrorPin `"0`"
`      RowLimit `"0`"
`      Columns `"COutputColumn`"
`      BEGIN DSSUBRECORD
`         Name `"WFG_CD`"
`         Description `"그룹내기관코드`"
`         SqlType `"12`"
`         Precision `"2`"
`         Scale `"0`"
`         Nullable `"0`"
`         KeyPosition `"1`"
`         DisplaySize `"0`"
`         Derivation `"\`"01\`"`"
`         Group `"0`"
`         ParsedDerivation `"\`"01\`"`"
`         SortKey `"0`"
`         SortType `"0`"
`         TableDef `"Database\\IFIN\\CDOWN.$ExcelTarTable`"
`         AllowCRLF `"0`"
`         LevelNo `"0`"
`         Occurs `"0`"
`         PadNulls `"0`"
`         SignOption `"0`"
`         SortingOrder `"0`"
`         ArrayHandling `"0`"
`         SyncIndicator `"0`"
`         PadChar `"`"
`         ColumnReference `"WFG_CD`"
`         ExtendedPrecision `"0`"
`         TaggedSubrec `"0`"
`         OccursVarying `"0`"
`         PKeyIsCaseless `"0`"
`         SCDPurpose `"0`"
`      END DSSUBRECORD
` $colArr2
`      BEGIN DSSUBRECORD
`         Name `"LOAD_PGM_ID`"
`         Description `"적재프로그램ID`"
`         SqlType `"12`"
`         Precision `"16`"
`         Scale `"0`"
`         Nullable `"0`"
`         KeyPosition `"0`"
`         DisplaySize `"0`"
`         Derivation `"sJobNm`"
`         Group `"0`"
`         ParsedDerivation `"sJobNm`"
`         SortKey `"0`"
`         SortType `"0`"
`         TableDef `"Database\\IFIN\\CDOWN.$ExcelTarTable`"
`         AllowCRLF `"0`"
`         LevelNo `"0`"
`         Occurs `"0`"
`         PadNulls `"0`"
`         SignOption `"0`"
`         SortingOrder `"0`"
`         StageVars `"sJobNm`"
`         ArrayHandling `"0`"
`         SyncIndicator `"0`"
`         PadChar `"`"
`         ColumnReference `"LOAD_PGM_ID`"
`         ExtendedPrecision `"0`"
`         TaggedSubrec `"0`"
`         OccursVarying `"0`"
`         PKeyIsCaseless `"0`"
`         SCDPurpose `"0`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"LOAD_DH`"
`         Description `"적재일시`"
`         SqlType `"12`"
`         Precision `"14`"
`         Scale `"0`"
`         Nullable `"0`"
`         KeyPosition `"0`"
`         DisplaySize `"0`"
`         Derivation `"RsLoadDH(0)`"
`         Group `"0`"
`         ParsedDerivation `"RsLoadDH(0)`"
`         Transform `"RsLoadDH`"
`         SortKey `"0`"
`         SortType `"0`"
`         TableDef `"Database\\IFIN\\CDOWN.WCDIFS15TH`"
`         AllowCRLF `"0`"
`         LevelNo `"0`"
`         Occurs `"0`"
`         PadNulls `"0`"
`         SignOption `"0`"
`         SortingOrder `"0`"
`         ArrayHandling `"0`"
`         SyncIndicator `"0`"
`         PadChar `"`"
`         ColumnReference `"LOAD_DH`"
`         ExtendedPrecision `"0`"
`         TaggedSubrec `"0`"
`         OccursVarying `"0`"
`         PKeyIsCaseless `"0`"
`         SCDPurpose `"0`"
`      END DSSUBRECORD
`      LeftTextPos `"375`"
`      TopTextPos `"192`"
`      LinkMinimised `"0`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S2`"
`      OLEType `"CCustomStage`"
`      Readonly `"0`"
`      Name `"T_$ExcelTarTable`"
`      NextID `"2`"
`      InputPins `"V6S2P1`"
`      StageType `"ORAOCI9`"
`      NLSMapName `"NONE`"
`      AllowColumnMapping `"0`"
`      Properties `"CCustomProperty`"
`      BEGIN DSSUBRECORD
`         Name `"DATABASE`"
`         Value `"#ParaETL.`$ETL_SID#`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"USERID`"
`         Value `"#ParaETL.`$ETL_UID#`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"PASSWORD`"
`         Value `"HD:@IJV@L93?0G7IL;JL0K87F5LIE@UD3U;<k?Kd?0QG1P7HH=0dG5`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"ISOPS`"
`         Value `"NO`"
`      END DSSUBRECORD
`      NextRecordID `"0`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S2P1`"
`      OLEType `"CCustomInput`"
`      Readonly `"0`"
`      Name `"T_01`"
`      Partner `"V6S1|V6S1P2`"
`      LinkType `"1`"
`      ConditionNotMet `"fail`"
`      LookupFail `"fail`"
`      Properties `"CCustomProperty`"
`      BEGIN DSSUBRECORD
`         Name `"TABLE`"
`         Value `"CDOWN.$ExcelTarTable`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"ISCASESENS`"
`         Value `"NO`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"GENSQL`"
`         Value `"Yes`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"USERSQL`"
`         Value `"NO`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"SQLBUILDERSQL`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"FULLYGENSQL`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"Pre42BINDING`"
`         Value `"Yes`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"ARRAYSIZE`"
`         Value `"10000`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"TRANSSIZE`"
`         Value `"20000`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"CREATETABLE`"
`         Value `"No`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"GENCREATEDDL`"
`         Value `"Yes`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"USERCREATEDDL`"
`         Value `"NO`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"DROPTABLE`"
`         Value `"No`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"GENDROPDDL`"
`         Value `"Yes`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"USERDROPDDL`"
`         Value `"DROP TABLE CDOWN.$ExcelTarTable`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"TABLESPACE`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"STORAGERULE`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"DODELETE`"
`         Value `"Trunc`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"DOUPDATE`"
`         Value `"Insert rows`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"TREATWARNASFATAL`"
`         Value `"NO`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"TX_ISO_LEVELI`"
`         Value `"Read Committed`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"BEFORESQL`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"CONTBEFORESQL`"
`         Value `"YES`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"AFTERSQL`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"CONTAFTERSQL`"
`         Value `"YES`"
`      END DSSUBRECORD
`      TransactionSize `"0`"
`      TXNBehaviour `"40`"
`      EnableTxGroup `"0`"
`      LinkMinimised `"0`"
`   END DSRECORD
`END DSJOB"
$dsjoball += $dsjob
$dsjob > C:\Users\woori\Documents\99.source\DSJOB\$ExcelTarTable"_J".dsx

$dsjobTmp  = "BEGIN HEADER
`   CharacterSet `"CP949`"
`   ExportingTool `"IBM InfoSphere DataStage Export`"
`   ToolVersion `"8`"
`   ServerName `"CDIDBT01`"
`   ToolInstanceID `"CDW_DEV`"
`   MDISVersion `"1.0`"
`   Date `"`"
`   Time `"`"
`   ServerVersion `"8.5`"
`END HEADER
`BEGIN DSJOB
`   Identifier `"$ExcelTarTable`_TEMP_J`"
`   DateModified `"`"
`   TimeModified `"`"
`   BEGIN DSRECORD
`      Identifier `"ROOT`"
`      OLEType `"CJobDefn`"
`      Readonly `"0`"
`      Name `"$ExcelTarTable`_TEMP_J`"
`      Description `"$ExcelTableNm`"
`      NextID `"7`"
`      Container `"V0`"
`      FullDescription `"1.소스     : $ExcelTarTable`_`$yyyymmdd.dat\(D)\(A)2.타겟     : $ExcelTarTable`_T01\(D)\(A)3.작성자   : $ExcelUserNm`\(D)\(A)4.적재방식 : $loadmanTemp`\(D)\(A)5.이력     : 2022.09.01 최초 생성\(D)\(A)6.설명     : $ExcelTableNm (변경적재)`"
`      JobVersion `"`"
`      BeforeSubr `"DSU.RsDwComn\\LB`"
`      AfterSubr `"DSU.RsDwComn\\LA`"
`      ControlAfterSubr `"0`"
`      Parameters `"CParameters`"
`      BEGIN DSSUBRECORD
`         Name `"ParaETL`"
`         Prompt `"ParaETL 매개변수`"
`         Default `"(As pre-defined)`"
`         HelpTxt `"카드DW 접속정보`"
`         ParamType `"13`"
`         ParamLength `"0`"
`         ParamScale `"0`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"`$yyyymmdd`"
`         Prompt `"yyyymmdd`"
`         ParamType `"0`"
`         ParamLength `"0`"
`         ParamScale `"0`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"`$HIRK_SEQ_INF`"
`         Prompt `"HIRK_SEQ_INF`"
`         ParamType `"0`"
`         ParamLength `"0`"
`         ParamScale `"0`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"`$POSI_SEQ_INF`"
`         Prompt `"POSI_SEQ_INF`"
`         ParamType `"0`"
`         ParamLength `"0`"
`         ParamScale `"0`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"`$IFIN_TMP_DIR`"
`         Prompt `"IFIN_TMP_DIR`"
`         Default `"`$PROJDEF`"
`         ParamType `"0`"
`         ParamLength `"0`"
`         ParamScale `"0`"
`      END DSSUBRECORD
`      MetaBag `"CMetaProperty`"
`      BEGIN DSSUBRECORD
`         Owner `"APT`"
`         Name `"AdvancedRuntimeOptions`"
`         Value `"#DSProjectARTOptions#`"
`      END DSSUBRECORD
`      NULLIndicatorPosition `"0`"
`      IsTemplate `"0`"
`      NLSLocale `",,,,`"
`      JobType `"0`"
`      Category `"\\Jobs\\IF_IN\\S.개인화마케팅`"
`      CenturyBreakYear `"30`"
`      NextAliasID `"2`"
`      ParameterFileDDName `"DD00001`"
`      ReservedWordCheck `"1`"
`      TransactionSize `"0`"
`      ValidationStatus `"0`"
`      Uploadable `"0`"
`      PgmCustomizationFlag `"0`"
`      JobReportFlag `"0`"
`      AllowMultipleInvocations `"0`"
`      Act2ActOverideDefaults `"0`"
`      Act2ActEnableRowBuffer `"0`"
`      Act2ActUseIPC `"0`"
`      Act2ActBufferSize `"0`"
`      Act2ActIPCTimeout `"0`"
`      ExpressionSemanticCheckFlag `"0`"
`      TraceOption `"0`"
`      EnableCacheSharing `"0`"
`      RuntimeColumnPropagation `"0`"
`      RelStagesInJobStatus `"-1`"
`      WebServiceEnabled `"0`"
`      MFProcessMetaData `"0`"
`      MFProcessMetaDataXMLFileExchangeMethod `"0`"
`      IMSProgType `"0`"
`      CopyLibPrefix `"ARDT`"
`      RecordPerformanceResults `"0`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V0`"
`      OLEType `"CContainerView`"
`      Readonly `"0`"
`      Name `"\(C791)\(C5C5)`"
`      NextID `"1`"
`      IsTopLevel `"0`"
`      StageList `"V6S0|V6S1|V6S2|V6A3`"
`      StageXPos `"사팔공|이육사|사팔공|이오`"
`      StageYPos `"오일구이|일구이|일구이|일`"
`      StageTypes `"CSeqFileStage|CTransformerStage|CCustomStage.ORAOCI9|ID_PALETTEJOBANNOTATION`"
`      NextStageID `"0`"
`      SnapToGrid `"1`"
`      GridLines `"0`"
`      ZoomValue `"100`"
`      StageXSize `"사팔|사팔|사팔523`"
`      StageYSize `"사팔|사팔|사팔119`"
`      ContainerViewSizing `"`"
`      StageNames `"S_$ExcelTarTable`_J|TR01|T_$ExcelTarTable`_T01| `"
`      StageTypeIDs `"CSeqFileStage|CTransformerStage|ORAOCI9| `"
`      LinkNames `"S_01|T_01| | `"
`      LinkHasMetaDatas `"True|True| | `"
`      LinkTypes `"1|1| | `"
`      LinkNamePositionXs `"`"
`      LinkNamePositionYs `"`"
`      TargetStageIDs `"V6S1|V6S2| | `"
`      SourceStageEffectiveExecutionModes `"0|0| | `"
`      SourceStageRuntimeExecutionModes `"0|0| | `"
`      TargetStageEffectiveExecutionModes `"0|0| | `"
`      TargetStageRuntimeExecutionModes `"0|0| | `"
`      LinkIsSingleOperatorLookup `"False|False| | `"
`      LinkIsSortSequential `"False|False| | `"
`      LinkSortMode `"0|0| | `"
`      LinkPartColMode `"0|0| | `"
`      LinkSourcePinIDs `"V6S0P1|V6S1P2| | `"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6A3`"
`      OLEType `"CAnnotation`"
`      Readonly `"0`"
`      Name `"V6A3`"
`      NextID `"0`"
`      AnnotationType `"2`"
`      TextFont `"\(AD74)\(B9BC)\(CCB4)\\10\\0\\0\\0\\400\\129`"
`      TextHorizontalJustification `"0`"
`      TextVerticalJustification `"0`"
`      TextColor `"0`"
`      BackgroundColor `"`"
`      BackgroundTransparent `"0`"
`      BorderVisible `"1`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S0`"
`      OLEType `"CSeqFileStage`"
`      Readonly `"0`"
`      Name `"S_$ExcelTarTable`_J`"
`      NextID `"2`"
`      OutputPins `"V6S0P1`"
`      UnixFormat `"1`"
`      NLSMapName `"NONE`"
`      PipeStage `"0`"
`      UnicodeBOM `"1`"
`      UnicodeSwapped `"1`"
`      AllowColumnMapping `"1`"
`      WithFilter `"0`"
`      StageType `"CSeqFileStage`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S0P1`"
`      OLEType `"CSeqOutput`"
`      Readonly `"0`"
`      Name `"S_01`"
`      Partner `"V6S1|V6S1P1`"
`      FileName `"#`$IFIN_TMP_DIR#/$ExcelTarTable`_TEMP_J.ifin_tmp`"
`      ColDelim `"|`"
`      QuoteChar `"000`"
`      ColHeaders `"0`"
`      FixedWidth `"0`"
`      ColSpace `"0`"
`      EnforceMetaData `"0`"
`      Readtimeout `"0`"
`      Columns `"COutputColumn`"
` $colArr
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S1`"
`      OLEType `"CTransformerStage`"
`      Readonly `"0`"
`      Name `"TR01`"
`      NextID `"3`"
`      InputPins `"V6S1P1`"
`      OutputPins `"V6S1P2`"
`      ValidationStatus `"0`"
`      StageType `"CTransformerStage`"
`      BlockSize `"0`"
`      StageVars `"CStageVar`"
`      BEGIN DSSUBRECORD
`         Name `"sJobNm`"
`         InitialValue `"DSJobName`"
`         SqlType `"3`"
`         Precision `"18`"
`         ColScale `"4`"
`         ExtendedPrecision `"0`"
`      END DSSUBRECORD
`      StageVarsMinimised `"0`"
`      LoopVarsMaximised `"0`"
`      MaxLoopIterations `"0`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S1P1`"
`      OLEType `"CTrxInput`"
`      Readonly `"0`"
`      Name `"S_01`"
`      Partner `"V6S0|V6S0P1`"
`      LinkType `"1`"
`      MultiRow `"0`"
`      LinkMinimised `"0`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S1P2`"
`      OLEType `"CTrxOutput`"
`      Readonly `"0`"
`      Name `"T_01`"
`      Partner `"V6S2|V6S2P1`"
`      Reject `"0`"
`      ErrorPin `"0`"
`      RowLimit `"0`"
`      Columns `"COutputColumn`"
`      BEGIN DSSUBRECORD
`         Name `"WFG_CD`"
`         Description `"그룹내기관코드`"
`         SqlType `"12`"
`         Precision `"2`"
`         Scale `"0`"
`         Nullable `"0`"
`         KeyPosition `"1`"
`         DisplaySize `"0`"
`         Derivation `"\`"01\`"`"
`         Group `"0`"
`         ParsedDerivation `"\`"01\`"`"
`         SortKey `"0`"
`         SortType `"0`"
`         TableDef `"Database\\wcscdw\\CDOWN.$ExcelTarTable`"
`         AllowCRLF `"0`"
`         LevelNo `"0`"
`         NativeType `"VARCHAR2`"
`         Occurs `"0`"
`         PadNulls `"0`"
`         SignOption `"0`"
`         SortingOrder `"0`"
`         ArrayHandling `"0`"
`         SyncIndicator `"0`"
`         PadChar `"`"
`         ColumnReference `"WFG_CD`"
`         ExtendedPrecision `"0`"
`         TaggedSubrec `"0`"
`         OccursVarying `"0`"
`         PKeyIsCaseless `"0`"
`         SCDPurpose `"0`"
`      END DSSUBRECORD
` $colArr2
`      BEGIN DSSUBRECORD
`         Name `"LOAD_PGM_ID`"
`         Description `"적재프로그램ID`"
`         SqlType `"12`"
`         Precision `"16`"
`         Scale `"0`"
`         Nullable `"0`"
`         KeyPosition `"0`"
`         DisplaySize `"0`"
`         Derivation `"\`"$ExcelTarTable`_J\`"`"
`         Group `"0`"
`         ParsedDerivation `"\`"$ExcelTarTable`_J\`"`"
`         SortKey `"0`"
`         SortType `"0`"
`         TableDef `"Database\\wcscdw\\CDOWN.$ExcelTarTable`"
`         AllowCRLF `"0`"
`         LevelNo `"0`"
`         NativeType `"VARCHAR2`"
`         Occurs `"0`"
`         PadNulls `"0`"
`         SignOption `"0`"
`         SortingOrder `"0`"
`         StageVars `"\`"$ExcelTarTable`_J\`"`"
`         ArrayHandling `"0`"
`         SyncIndicator `"0`"
`         PadChar `"`"
`         ColumnReference `"LOAD_PGM_ID`"
`         ExtendedPrecision `"0`"
`         TaggedSubrec `"0`"
`         OccursVarying `"0`"
`         PKeyIsCaseless `"0`"
`         SCDPurpose `"0`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"LOAD_DH`"
`         Description `"적재일시`"
`         SqlType `"12`"
`         Precision `"14`"
`         Scale `"0`"
`         Nullable `"0`"
`         KeyPosition `"0`"
`         DisplaySize `"0`"
`         Derivation `"RsLoadDH(0)`"
`         Group `"0`"
`         ParsedDerivation `"RsLoadDH(0)`"
`         Transform `"RsLoadDH`"
`         SortKey `"0`"
`         SortType `"0`"
`         TableDef `"Database\\wcscdw\\CDOWN.$ExcelTarTable`"
`         AllowCRLF `"0`"
`         LevelNo `"0`"
`         NativeType `"VARCHAR2`"
`         Occurs `"0`"
`         PadNulls `"0`"
`         SignOption `"0`"
`         SortingOrder `"0`"
`         ArrayHandling `"0`"
`         SyncIndicator `"0`"
`         PadChar `"`"
`         ColumnReference `"LOAD_DH`"
`         ExtendedPrecision `"0`"
`         TaggedSubrec `"0`"
`         OccursVarying `"0`"
`         PKeyIsCaseless `"0`"
`         SCDPurpose `"0`"
`      END DSSUBRECORD
`      LeftTextPos `"375`"
`      TopTextPos `"192`"
`      LinkMinimised `"0`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S2`"
`      OLEType `"CCustomStage`"
`      Readonly `"0`"
`      Name `"T_$ExcelTarTable`_T01`"
`      NextID `"2`"
`      InputPins `"V6S2P1`"
`      StageType `"ORAOCI9`"
`      NLSMapName `"NONE`"
`      AllowColumnMapping `"0`"
`      Properties `"CCustomProperty`"
`      BEGIN DSSUBRECORD
`         Name `"DATABASE`"
`         Value `"#ParaETL.`$ETL_SID#`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"USERID`"
`         Value `"#ParaETL.`$ETL_UID#`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"PASSWORD`"
`         Value `"HD:@IJV@L93?0G7IL;JL0K87F5LIE@UD3U;<k?Kd?0QG1P7HH=0dG5`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"ISOPS`"
`         Value `"NO`"
`      END DSSUBRECORD
`      NextRecordID `"0`"
`   END DSRECORD
`   BEGIN DSRECORD
`      Identifier `"V6S2P1`"
`      OLEType `"CCustomInput`"
`      Readonly `"0`"
`      Name `"T_01`"
`      Partner `"V6S1|V6S1P2`"
`      LinkType `"1`"
`      ConditionNotMet `"fail`"
`      LookupFail `"fail`"
`      Properties `"CCustomProperty`"
`      BEGIN DSSUBRECORD
`         Name `"TABLE`"
`         Value `"CDTMP.$ExcelTarTable`_T01`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"ISCASESENS`"
`         Value `"NO`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"GENSQL`"
`         Value `"Yes`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"USERSQL`"
`         Value `"NO`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"SQLBUILDERSQL`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"FULLYGENSQL`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"Pre42BINDING`"
`         Value `"Yes`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"ARRAYSIZE`"
`         Value `"10000`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"TRANSSIZE`"
`         Value `"20000`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"CREATETABLE`"
`         Value `"No`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"GENCREATEDDL`"
`         Value `"Yes`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"USERCREATEDDL`"
`         Value `"NO`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"DROPTABLE`"
`         Value `"No`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"GENDROPDDL`"
`         Value `"Yes`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"USERDROPDDL`"
`         Value `"DROP TABLE CDTMP.$ExcelTarTable`_T01`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"TABLESPACE`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"STORAGERULE`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"DODELETE`"
`         Value `"Trunc`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"DOUPDATE`"
`         Value `"Insert rows`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"TREATWARNASFATAL`"
`         Value `"NO`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"TX_ISO_LEVELI`"
`         Value `"Read Committed`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"BEFORESQL`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"CONTBEFORESQL`"
`         Value `"YES`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"AFTERSQL`"
`      END DSSUBRECORD
`      BEGIN DSSUBRECORD
`         Name `"CONTAFTERSQL`"
`         Value `"YES`"
`      END DSSUBRECORD
`      TransactionSize `"0`"
`      TXNBehaviour `"40`"
`      EnableTxGroup `"0`"
`      LinkMinimised `"0`"
`   END DSRECORD
`END DSJOB"
$dsjoball += $dsjobTmp
            $dsjobTmp > C:\Users\woori\Documents\99.source\DSJOB\$ExcelTarTable"_TEMP_J".dsx
        }
     }
     $dsjoball > C:\Users\woori\Documents\99.source\DSJOB\WCDIFS_JOB_ALL_J.dsx
     $ExcelWorkBook.Close()
     $con.close()
     Write-Host "=================================================================================================="
     Write-Host "=============================================작업완료============================================="
     Write-Host "=================================================================================================="
} catch {
     Write-Error ("Database Exception:{0}{1}" -f ` $con.ConnectionString, $_.Exception.ToString())
} finally{
     if ($con.State -eq 'Open') {
        $con.close() 
     }
}