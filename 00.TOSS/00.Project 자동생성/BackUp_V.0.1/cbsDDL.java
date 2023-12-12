public class CBSConnection {

              public String CBSDDL() {


                            Connection connCbs \u003d null;
                            PreparedStatement pstmCbs \u003d null;
                            PreparedStatement pstmCbsPart \u003d null;
                            ResultSet cbsRs \u003d null;
                            ResultSet cbsPartRs \u003d null;

                            try {

                                          String cbsQuary \u003d \"SELECT  \\r\
\"
                                                        + \"                     A.OWNER\\r\
\"
                                                        + \"                     ,A.TABLE_NAME\\r\
\"
                                                        + \"                     ,B.COMMENTS AS TABLE_COMMENTS\\r\
\"
                                                        + \"                     ,C.COMMENTS AS COLUMN_COMMENTS\\r\
\"
                                                        + \"                     ,A.DATA_TYPE\\r\
\"
                                                        + \"                     ,A.DATA_LENGTH\\r\
\"
                                                        + \"                     ,A.DATA_PRECISION\\r\
\"
                                                        + \"                     ,A.DATA_SCALE\\r\
\"
                                                        + \"                     ,A.NULLABLE\\r\
\"
                                                        + \"                     ,A.COLUMN_ID\\r\
\"
                                                        + \"                     ,A.DEFAULT_LENGTH\\r\
\"
                                                        + \"                     ,TRIM(REPLACE(DBMS_XMLGEN.GETXMLTYPE(\u0027SELECT DATA_DEFAULT FROM ALL_TAB_COLS WHERE OWNER\u003d\u0027\u0027\u0027||A.OWNER||\u0027\u0027\u0027 AND TABLE_NAME \u003d \u0027\u0027\u0027||A.TABLE_NAME||\u0027\u0027\u0027 AND COLUMN_NAME \u003d \u0027\u0027\u0027||A.COLUMN_NAME||\u0027\u0027\u0027 \u0027).EXTRACT(\u0027//text()\u0027),\u0027\u0026apos;\u0027,\u0027\u0027\u0027\u0027)) AS DATA_DEFAULT\\r\
\"
                                                        + \"                     ,(CASE WHEN D.COLUMN_NAME IS NULL THEN \u0027N\u0027 ELSE \u0027Y\u0027 END) AS PK_YN\\r\
\"
                                                        + \"            FROM ALL_TAB_COLS A\\r\
\"
                                                        + \"            LEFT \\r\
\"
                                                        + \"             JOIN ALL_TAB_COMMENTS B\\r\
\"
                                                        + \"                  ON B.OWNER \u003d A.OWNER\\r\
\"
                                                        + \"               AND B.TABLE_NAME \u003d A.TABLE_NAME\\r\
\"
                                                        + \"              LEFT \\r\
\"
                                                        + \"              JOIN ALL_COL_COMMENTS C\\r\
\"
                                                        + \"                 ON C.OWNER \u003d A.OWNER\\r\
\"
                                                        + \"              AND C.TABLE_NAME \u003d A.TABLE_NAME\\r\
\"
                                                        + \"               AND C.COLUMN_NAME \u003d A.COLUMN_NAME\\r\
\"
                                                        + \"              LEFT \\r\
\"
                                                        + \"              JOIN ALL_IND_COLUMNS D \\r\
\"
                                                        + \"                   ON D.TABLE_OWNER \u003d A.OWNER\\r\
\"
                                                        + \"                  AND D.TABLE_NAME \u003d A.TABLE_NAME\\r\
\"
                                                        + \"                  AND D.COLUMN_NAME \u003d A.COLUMN_NAME\\r\
\"
                                                        + \"                   AND (D.INDEX_NAME LIKE \u0027PK%\u0027 OR D.INDEX_NAME LIKE \u0027%PK\u0027)\\r\
\"
                                                        + \"               WHERE A.OWNER \u003d \u0027CBSOWN\u0027\\r\
\"
                                                        + \"                   AND A.TABLE_NAME \u003d ?\\r\
\"
                                                        + \"                   AND A.COLUMN_NAME NOT LIKE \u0027%PSWD\u0027\\r\
\"
                                                        + \"                   AND A.HIDDEN_COLUMN \u003d \u0027NO\u0027\\r\
\"
                                                        + \"                ORDER BY A.OWNER, A.TABLE_NAME, A.COLUMN_ID\";

                                            String cbsPartQuary \u003d \"SELECT \\r\
\"
                                                        + \"                     TABLE_OWNER\\r\
\"
                                                        + \"                    ,TABLE_NAME\\r\
\"
                                                        + \"                    ,PARTITION_NAME\\r\
\"
                                                        + \"                     ,TRIM(REPLACE(DBMS_XMLGEN.GETXMLTYPE(\u0027SELECT HIGH_VALUE FROM ALL_TAB_PARTITIONS WHERE TABLE_OWNER\u003d\u0027\u0027\u0027||A.TABLE_OWNER||\u0027\u0027\u0027 AND TABLE_NAME \u003d \u0027\u0027\u0027||A.TABLE_NAME||\u0027\u0027\u0027 AND PARTITION_NAME \u003d \u0027\u0027\u0027||A.PARTITION_NAME||\u0027\u0027\u0027 \u0027).EXTRACT(\u0027//text()\u0027),\u0027\u0026apos;\u0027,\u0027\u0027\u0027\u0027)) AS HIGH_VALUE\\r\
\"
                                                        + \"                    ,ROWNUM AS RN\\r\
\"
                                                        + \"        FROM ALL_TAB_PARTITIONS A\\r\
\"
                                                        + \"      WHERE TABLE_OWNER \u003d \u0027CBSOWN\u0027\\r\
\"
                                                        + \"             AND TABLE_NAME \u003d ?\\r\
\"
                                                        + \"      ORDER BY TABLE_OWNER,TABLE_NAME,PARTITION_NAME\";

                                          int rowIndex \u003d 2;
                                          int allChangeCount \u003d 0;
                                          int notChangeCount \u003d 0;
                                          int delTableCount \u003d 0;
                                          int ifTableCount \u003d 0;
                                          int snpTableCount \u003d 0;

                                          FileInputStream file \u003d new FileInputStream(\"엑셀경로\");
                                          XSSFWorkbook workBook \u003d new XSSFWorkbook(file);
                                          XSSFSheet sheet \u003d workBook.getSheet(\"시트명\");
                                          int rows \u003d sheet.getPhysicalNumberOfRows();


                                          connCbs \u003d DBConnection.getConnection();

                                          String allCbsResult \u003d \"\";
                                          String chgCbsResult \u003d \"\";
                                          String allFilePath \u003d \"전체 DDL 저장 경로\";
                                          String chgFilePath \u003d \"변경 DDL 저장 경로\";

                                         for (rowIndex \u003d 2; rowIndex \u003c rows; rowIndex++) {

                                                        String tblOwner \u003d \"STGOWN\";

                                                        XSSFRow row \u003d sheet.getRow(rowIndex);
                                                        XSSFCell tblNo \u003d row.getCell(0);
                                                        XSSFCell tblId \u003d row.getCell(5);
                                                        XSSFCell tblNm \u003d row.getCell(6);
                                                        XSSFCell tblGb \u003d row.getCell(7);
                                                        XSSFCell tblPart \u003d row.getCell(16);
                                                        XSSFCell tblloadchk \u003d row.getCell(17);

                                                        String snpsChk \u003d tblPart.getStringCellvalue().trim();
                                                        String gbChg \u003d tblGb.getStringCellValue().trim();
                                                        String tblName \u003d tblNm.getStringCellValue();

                                                        int changeCount \u003d 0;

                                                        if (snpsChk.equals(\"SNPSH_BASE_DT\")) {

                                                                      snpTableCount++;
                                                                      continue;
                                                        }

                                                        if (gbChk.equals(\"삭제\")) {
                                                                      delTableCount++;
                                                                      continue;
                                                        }

                                                        String loadchk \u003d tblloadchk.getStringCellValue().trim();

                                                        if (loadchk.equals(\"I/F적재형\")) {
                                                                      ifTableCount++;
                                                                      continue;
                                                        }

                                                        String filePath \u003d \"D:\\\\#PGM\\\\#작업\\\\java\\\\CBS\\\\STG_DDL\\\\\"+tblId+\".sql\";

                                                        pstmCbs \u003d connCbs.prepareStatement(cbsQuary);
                                                        pstmCbs.setString(1, \"C\" + tblID.getStringCellValue().substring(1));
                                                        cbsRs \u003d pstmCbs.executeQuery();

                                                        String cbsResult \u003d null;
                                                        String colInfos \u003d \"\";
                                                        String partYn \u003d \"\";
                                                        String colnmCos \u003d \"\";

                                                        while (cbsRs.next()) {
                                                                      String cbsOwner \u003d cbsRs.getString(\"OWNER\");
                                                                      String cbsTblNm \u003d cbsRs.getString(\"TABLE_NAME\");
                                                                      String cbsTblCo \u003d cbsRs.getString(\"TABLE_COMMENTS\");
                                                                      String cbsColumn \u003d cbsRs.getString(\"COLUMN_NAME\");
                                                                      String cbsColumnCo \u003d cbsRs.getString(\"COLUMN_COMMENTS\");
                                                                      String cbsType \u003d cbsRs.getString(\"DATA_TYPE\");
                                                                      String cbsLength \u003d cbsRs.getString(\"DATA_LENGTH\");
                                                                      String cbsPrecision \u003d cbsRs.getString(\"DATA_PRECISION\");
                                                                      String cbsScale \u003d cbsRs.getString(\"DATA_SCALE\");
                                                                      String cbsNull \u003d cbsRs.getString(\"NULLABLE\");
                                                                      int cbsId \u003d cbsRs.getInt(\"COLUMN_ID\");
                                                                      String cbsDefaultLen \u003d cbsRs.getString(\"DEFAULT_LENGTH\");
                                                                      String cbsDataDefault \u003d cbsRs.getString(\"DATA_DEFAULT\").trim();
                                                                      String cbsPk \u003d cbsRs.getString(\"PK_YN\");

                                                                      String colType \u003d \"\";
                                                                      String colDef \u003d \"\";
                                                                      String colNull \u003d \"\";
                                                                      String colInfo \u003d \"      \";
                                                                      String colNmco \u003d \"\";

                                                                      if (cbsType.contains(\"TIMESTAMP\")) {
                                                                                    colType \u003d cbsType;
                                                                      } else if (cbsType.contains(\"VARCHAR\")) {
                                                                                    colType \u003d cbsType + \"(\" + cbsLength + \")\";
                                                                      } else if ( cbsType.contains(\"NUMBER\") \u0026\u0026 cbsScale \u003d\u003d null) {
                                                                                    colType \u003d cbsType + \"(\" + cbsPrecision + \")\";
                                                                      } else if (cbsType.contains(\"NUMBER\")) }
                                                                                    colType \u003d cbsType + \"(\" + cbsPrecision + \",\" + cbsScale + \")\";
                                                                      }

                                                                      if (cbsDataDefaule.length() !\u003d 0) {
                                                                                    colDef \u003d \"DEFAULT \" + cbsDataDefault;
                                                                      }

                                                                      if (cbsNull.equals(\"Y\")) {
                                                                                    colNull +\u003d \"    NULL\";
                                                                      } else {
                                                                                    colNull +\u003d \"NOT NULL\";
                                                                      }

                                                                      if (cbsId \u003d\u003d 1) {
                                                                                    colInfo +\u003d \" \";
                                                                      } else {
                                                                                    colInfo +\u003d \",\";
                                                                      }

                                                                      colInfo +\u003d rPad.padRight(cbsColumn, 30m \u0027 \u0027) + rPad.padRight(colType, 18, \u0027 \u0027) + rPad.padRight(colDef, 28, \u0027 \u0027) + colNull;

                                                                      colInfos +\u003d colInfo + \"\\r\
\";

                                                                      colNmco \u003d \"COMMENT ON COLUMN \" + tblOwner + \".\" + tblId + \".\" + rPad.padRight(cbsColumn, 30, \u0027\u0027) + \" IS  \u0027\" + cbsColumnCo + \"\u0027;\";

                                                                      colnmCos +\u003d colNmco + \"\\r\
\";
                                                        }

                                                        if (snpsChk.length() !\u003d 0 \u0026\u0026 !snpsChk.equals(\"SNPSH_BASE_DT\")) { 

pstmCbsPart \u003d connCbs.prepareStatement(cbsPartQuary,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
pstmCbsPart.setString(1,\"C\" + tblId.getStringCellValue().substring(1));
cbsPartRs \u003d pstmCbsPart.executeQuery();

cbsPartRs.last();
int cbsPartrowCount \u003d 0;
cbsPartrowCount \u003d cbsPartRs.getRow();

if (cbsPartrowCount !\u003d 0 ) {
partYn \u003d \"SEGMENT CREATION IMMEDIATE\\r\
\"
+ \"PCTFREE 0\\r\
\"
+ \"STORAGE\\r\
\"
+ \"(\\r\
\"
+ \"INITIAL 1M\\r\
\"
+ \"MAXEXTENTS 2147483645\\r\

+ \"PCTINCREASE 0\\r\
\"
+ \")\\r\
\"
+ \"TABLESPACE CSTGST01\";
}
cbsPartRs.beforeFirst();
}


cbsResult \u003d \"/**\"+ tblID + \" : \" + tblName + \"**/\\r\
\"
+ \"\\r\
\"
+ \"BEGIN EXECUTE IMMEDIATE \u0027DROP TABLE \" + tblOwner + \".\" + tblId + \" CASCADE CONSTRAINTS PURGE\u0027; EXCEPTION WHEN OTHERS THEN NULL; END;/\"
+ \"\\r\
\"
+ \"CREATE TABLE \" + tblOwner + \".\" + tblId + \"\\r\
\"
+ \"(\\r\
\"
+ colInfos
+ \")\\r\
\"
+ partYn + \"\\r\
\"
+ \";\\r\
\"
+ \"\\r\
\"
+ \"\\r\
\"
+ \"GRANT SELECT ON \" + tblOwner + \".\" + tblId + \" TO RL_SOR_SEL;\\r\
\"
+ \"GRANT INSERT, UPDATE, DELETE, SELECT ON \" + tblOnwer + \".\" + tblId + \" TO RL_SOR_ALL;\\r\
\"
+ \"\\r\
\"
+ \"\\r\
\"
+ \"COMMENT ON TABLE \" + tblOwner + \".\" + tblId + \" IS \u0027\" + tblName + \"\u0027;\\r\
\"
+ \"\\r\
\"
+ colnmCos + \"\\r\
\"
+ \"COMMIT;\\r\
\"
+ \"\\r\
\"
+ \"\\r\
\"
+ \"\\r\
\";


allCbsResult +\u003d cbsResult + \"\\r\
\" + \"\\r\
\";


File file1 \u003d new File(filePath);
if (!file1.exists()) {

FileOutputStream fileOutStream \u003d new FileOutputStream(filePath);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \"UTF8\");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(cbsResult);
chgCbsResult +\u003d cbsResult + \"\\r\
\" + \"\\r\
\";
bufferedWriter.close();
changeCount ++;
} else {
FileInputStream fileInStream \u003d new FileInputStream(filePath);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \"UTF8\");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;
String oldFile \u003d \"\";

while((ch \u003d bufferedReader.read()) !\u003d -1) {

oldFile +\u003d (char)ch;
}

if (oldFile.equals(cbsResult)) {

} else if (!oldFile.equals(cbsResult) \u0026\u0026 file1.exists()) {
FileOutputStream fileOutStream \u003d new FileOutputStream(filePath);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \"UTF8\");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(cbsResult);
chgCbsResult +\u003d cbsResult + \"\\r\
\" + \"\\r\
\";
bufferedWriter.close();
changeCount ++;
}
}
if (changeCount \u003d\u003d 0) {
System.out.println(\"[\" + tblNo + \"]\" + \"[\" + tblId + \"]\" + \"[\" + tblName + \"]\"  + \" [DDL 변경없음]\");
notChangeCount++;
} else {
System.out.println(\"[\" + tblNo + \"]\" + \"[\" + tblId + \"]\" + \"[\" + tblName + \"]\"  + \" [DDL 생성완료]\");
allChangeCount++;
}

}

int allTableCount \u003d allChangeCount + notChangeCount + delTableCount + ifTableCount + snpTableCount;
int crtnTableCount \u003d allChangeCount + notChangeCount;

BufferedOutputStream allFileWriter \u003d new BufferedOutputStream(new FileOutputStream(allFilePath))l
allfileWriter.write(allCbsResult.getBytes());
allfileWriter.close();

System.out,println(\"\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\");
System.out,println(\"[전체 테이블 \" + allTableCount + \" 건 테이블 중 총 \" + crtnTableCount + \" 건 DDL이. 생성되었습니다.]\");

BufferedOutputStream chgfileWriter \u003d new BufferedOutputStream(new FileOutputStream(chgFilePath));
chgFileWriter.write(chgCbsResult.getBytes());
chgfileWriter.close();
System.out.println(\"[변경된 DDL이 \" + allChangeCount + \" 건 생성되었습니다.]\");
System.out,println(\"\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\");

workBook.close();

} catch (SQLException | IOException sqle) {
System.out.println(\"예외발생\");
sqle.printStackTrace();
} finally {
try {
if (cbsRs !\u003d null) {
cbsRs.close();
}
if (pstmCbs !\u003d null) {
pstmCbs.close();
}
if (connCbs !\u003d null) {
connCbs.close();
}
}catch (Exception e){
throw new RuntimeException(e.getMessage());
}
}
return \"\";
}
}




