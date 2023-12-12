public class GenTeraBlock {

public String Terablock() {

                            Connection connCbs \u003d null;
                            PreparedStatement pstmCbs \u003d null;
                            PreparedStatement pstmCbsPart \u003d null;
                            ResultSet cbsRs \u003d null;
                            ResultSet cbsPartRs \u003d null;
ResultSetMetaData count \u003d null;
PreparedStatement partcbs \u003d null;
Connection connsor \u003d null;
                            PreparedStatement pstmsor \u003d null;
                            PreparedStatement pstmsorPart \u003d null;
                            ResultSet sorRs \u003d null;
                            ResultSet sorPartRs \u003d null;



                            try {

                                          String cbsquary \u003d \""SELECT  \\r\
\""
                                                        + \""                     A.OWNER\\r\
\""
                                                        + \""                     ,A.TABLE_NAME\\r\
\""
                                                        + \""                     ,B.COMMENTS AS TABLE_COMMENTS\\r\
\""
                                                        + \""                     ,C.COMMENTS AS COLUMN_COMMENTS\\r\
\""
                                                        + \""                     ,A.DATA_TYPE\\r\
\""
                                                        + \""                     ,A.DATA_LENGTH\\r\
\""
                                                        + \""                     ,A.DATA_PRECISION\\r\
\""
                                                        + \""                     ,A.DATA_SCALE\\r\
\""
                                                        + \""                     ,A.NULLABLE\\r\
\""
                                                        + \""                     ,A.COLUMN_ID\\r\
\""
                                                        + \""                     ,A.DEFAULT_LENGTH\\r\
\""
                                                        + \""                     ,TRIM(REPLACE(DBMS_XMLGEN.GETXMLTYPE(\u0027SELECT DATA_DEFAULT FROM ALL_TAB_COLS WHERE OWNER\u003d\u0027\u0027\u0027||A.OWNER||\u0027\u0027\u0027 AND TABLE_NAME \u003d \u0027\u0027\u0027||A.TABLE_NAME||\u0027\u0027\u0027 AND COLUMN_NAME \u003d \u0027\u0027\u0027||A.COLUMN_NAME||\u0027\u0027\u0027 \u0027).EXTRACT(\u0027//text()\u0027),\u0027\u0026apos;\u0027,\u0027\u0027\u0027\u0027)) AS DATA_DEFAULT\\r\
\""
                                                        + \""                     ,(CASE WHEN D.COLUMN_NAME IS NULL THEN \u0027N\u0027 ELSE \u0027Y\u0027 END) AS PK_YN\\r\
\""
                                                        + \""            FROM ALL_TAB_COLS A\\r\
\""
                                                        + \""            LEFT \\r\
\""
                                                        + \""             JOIN ALL_TAB_COMMENTS B\\r\
\""
                                                        + \""                  ON B.OWNER \u003d A.OWNER\\r\
\""
                                                        + \""               AND B.TABLE_NAME \u003d A.TABLE_NAME\\r\
\""
                                                        + \""              LEFT \\r\
\""
                                                        + \""              JOIN ALL_COL_COMMENTS C\\r\
\""
                                                        + \""                 ON C.OWNER \u003d A.OWNER\\r\
\""
                                                        + \""              AND C.TABLE_NAME \u003d A.TABLE_NAME\\r\
\""
                                                        + \""               AND C.COLUMN_NAME \u003d A.COLUMN_NAME\\r\
\""
                                                        + \""              LEFT \\r\
\""
                                                        + \""              JOIN ALL_IND_COLUMNS D \\r\
\""
                                                        + \""                   ON D.TABLE_OWNER \u003d A.OWNER\\r\
\""
                                                        + \""                  AND D.TABLE_NAME \u003d A.TABLE_NAME\\r\
\""
                                                        + \""                  AND D.COLUMN_NAME \u003d A.COLUMN_NAME\\r\
\""
                                                        + \""                   AND (D.INDEX_NAME LIKE \u0027PK%\u0027 OR D.INDEX_NAME LIKE \u0027%PK\u0027)\\r\
\""
                                                        + \""               WHERE A.OWNER \u003d \u0027CBSOWN\u0027\\r\
\""
                                                        + \""                   AND A.TABLE_NAME \u003d ?\\r\
\""
                                                        + \""                   AND A.COLUMN_NAME NOT LIKE \u0027%PSWD\u0027\\r\
\""
                                                        + \""                   AND A.HIDDEN_COLUMN \u003d \u0027NO\u0027\\r\
\""
                                                        + \""                ORDER BY A.OWNER, A.TABLE_NAME, A.COLUMN_ID\"";

                                            String cbspartquary \u003d \""SELECT \\r\
\""
                                                        + \""                     TABLE_OWNER\\r\
\""
                                                        + \""                    ,TABLE_NAME\\r\
\""
                                                        + \""                    ,PARTITION_NAME\\r\
\""
                                                        + \""                     ,TRIM(REPLACE(DBMS_XMLGEN.GETXMLTYPE(\u0027SELECT HIGH_VALUE FROM ALL_TAB_PARTITIONS WHERE TABLE_OWNER\u003d\u0027\u0027\u0027||A.TABLE_OWNER||\u0027\u0027\u0027 AND TABLE_NAME \u003d \u0027\u0027\u0027||A.TABLE_NAME||\u0027\u0027\u0027 AND PARTITION_NAME \u003d \u0027\u0027\u0027||A.PARTITION_NAME||\u0027\u0027\u0027 \u0027).EXTRACT(\u0027//text()\u0027),\u0027\u0026apos;\u0027,\u0027\u0027\u0027\u0027)) AS HIGH_VALUE\\r\
\""
                                                        + \""                    ,ROWNUM AS RN\\r\
\""
                                                        + \""        FROM ALL_TAB_PARTITIONS A\\r\
\""
                                                        + \""      WHERE TABLE_OWNER \u003d \u0027CBSOWN\u0027\\r\
\""
                                                        + \""             AND TABLE_NAME \u003d ?\\r\
\""
                                                        + \""      ORDER BY TABLE_OWNER,TABLE_NAME,PARTITION_NAME\"";


                                          String sorquary \u003d \""SELECT  \\r\
\""
                                                        + \""                     A.OWNER\\r\
\""
                                                        + \""                     ,A.TABLE_NAME\\r\
\""
                                                        + \""                     ,B.COMMENTS AS TABLE_COMMENTS\\r\
\""
                                                        + \""                     ,C.COMMENTS AS COLUMN_COMMENTS\\r\
\""
                                                        + \""                     ,A.DATA_TYPE\\r\
\""
                                                        + \""                     ,A.DATA_LENGTH\\r\
\""
                                                        + \""                     ,A.DATA_PRECISION\\r\
\""
                                                        + \""                     ,A.DATA_SCALE\\r\
\""
                                                        + \""                     ,A.NULLABLE\\r\
\""
                                                        + \""                     ,A.COLUMN_ID\\r\
\""
                                                        + \""                     ,A.DEFAULT_LENGTH\\r\
\""
                                                        + \""                     ,TRIM(REPLACE(DBMS_XMLGEN.GETXMLTYPE(\u0027SELECT DATA_DEFAULT FROM ALL_TAB_COLS WHERE OWNER\u003d\u0027\u0027\u0027||A.OWNER||\u0027\u0027\u0027 AND TABLE_NAME \u003d \u0027\u0027\u0027||A.TABLE_NAME||\u0027\u0027\u0027 AND COLUMN_NAME \u003d \u0027\u0027\u0027||A.COLUMN_NAME||\u0027\u0027\u0027 \u0027).EXTRACT(\u0027//text()\u0027),\u0027\u0026apos;\u0027,\u0027\u0027\u0027\u0027)) AS DATA_DEFAULT\\r\
\""
                                                        + \""                     ,(CASE WHEN D.COLUMN_NAME IS NULL THEN \u0027N\u0027 ELSE \u0027Y\u0027 END) AS PK_YN\\r\
\""
+\""             ,MAX(LENGTH(A.COLUMN_NAME)) OVER() AS MAX_COL_LEN\\r\
\""
+\""             ,MAX(LENGTH(D.COLUMN_NAME)) OVER() AS PK_MAX_COL_LEN\\r\
\""
                                                        + \""            FROM ALL_TAB_COLS A\\r\
\""
                                                        + \""            LEFT \\r\
\""
                                                        + \""             JOIN ALL_TAB_COMMENTS B\\r\
\""
                                                        + \""                  ON B.OWNER \u003d A.OWNER\\r\
\""
                                                        + \""               AND B.TABLE_NAME \u003d A.TABLE_NAME\\r\
\""
                                                        + \""              LEFT \\r\
\""
                                                        + \""              JOIN ALL_COL_COMMENTS C\\r\
\""
                                                        + \""                 ON C.OWNER \u003d A.OWNER\\r\
\""
                                                        + \""              AND C.TABLE_NAME \u003d A.TABLE_NAME\\r\
\""
                                                        + \""               AND C.COLUMN_NAME \u003d A.COLUMN_NAME\\r\
\""
                                                        + \""              LEFT \\r\
\""
                                                        + \""              JOIN ALL_IND_COLUMNS D \\r\
\""
                                                        + \""                   ON D.TABLE_OWNER \u003d A.OWNER\\r\
\""
                                                        + \""                  AND D.TABLE_NAME \u003d A.TABLE_NAME\\r\
\""
                                                        + \""                  AND D.COLUMN_NAME \u003d A.COLUMN_NAME\\r\
\""
                                                        + \""                   AND (D.INDEX_NAME LIKE \u0027PK%\u0027 OR D.INDEX_NAME LIKE \u0027%PK\u0027)\\r\
\""
                                                        + \""               WHERE A.OWNER \u003d \u0027CBSOWN\u0027\\r\
\""
                                                        + \""                   AND A.TABLE_NAME \u003d ?\\r\
\""
                                                        + \""                   AND A.HIDDEN_COLUMN \u003d \u0027NO\u0027\\r\
\""
                                                        + \""                ORDER BY A.OWNER, A.TABLE_NAME, A.COLUMN_ID\"";


                                          int rowIndex \u003d 2;
                                          int allChangeCount \u003d 0;
                                          int notChangeCount \u003d 0;
                                          int delTableCount \u003d 0;
                                          int ifTableCount \u003d 0;
                                          int snpTableCount \u003d 0;

                                          FileInputStream file \u003d new FileInputStream(\""엑셀경로\"");
                                          XSSFWorkbook workBook \u003d new XSSFWorkbook(file);
                                          XSSFSheet sheet \u003d workBook.getSheet(\""시트명\"");
                                          int rows \u003d sheet.getPhysicalNumberOfRows();


                                          conncbs \u003d DBConnection.getConnection();
connsor \u003d SORDBConnection.getConnection();

                                         for (rowIndex \u003d 2; rowIndex \u003c rows; rowIndex++) {

                                                        XSSFRow row \u003d sheet.getRow(rowIndex);
                                                        XSSFCell tblno \u003d row.getCell(0);
                                                        XSSFCell tblID \u003d row.getCell(5);
                                                        XSSFCell tblNM \u003d row.getCell(6);
                                                        XSSFCell tblgb \u003d row.getCell(7);
                                                        XSSFCell tblpart \u003d row.getCell(16);
                                                        XSSFCell tblloadchk \u003d row.getCell(17);
XSSFCell tblSnap \u003d row.getCell(23);
XSSFCell tblSub \u003d row.getCell(26);
XSSFCell cbstbl \u003d row.getCell(21);
XSSFCell sUCD \u003d row.getCell(32);
XSSFCell sLCD \u003d row.getCell(35);
XSSFCell sUID \u003d row.getCell(38);
XSSFCell sLID \u003d row.getCell(41);

                                                        String snpschk \u003d tblpart.getStringCellvalue().trim();
                                                        String gbchk \u003d tblgb.getStringCellValue().trim();
                                                        String tblname \u003d tblNM.getStringCellValue();
String tblsnps \u003d tblSnap.getStringCellValue();
String tblname \u003d tblNM.getStringCellValue();
String sub \u003d tblSub.getStringCellValue().trim();
String UCD \u003d sUCD.getStringCellValue();
String LCD \u003d sLCD.getStringCellValue();
String UID \u003d sUID.getStringCellValue();
String LID \u003d sLID.getStringCellValue();

pstmsor \u003d connsor.prepareStatement(sorquary);
pstmsor.setString(1, tblID.getStringCellValue());

pstmcbs \u003d conncbs.prepareStatement(cbsquary);
pstmcbs.setString(1, \""C\"" + tblID.getStringCellValue().substring(1));

                                                        int changeCount \u003d 0;

                                                        if (gbChk.equals(\""삭제\"")) {
                                                                      delTableCount++;
                                                                      continue;
                                                        }

                                                        String loadchk \u003d tblloadchk.getStringCellValue().trim();

                                                        if (loadchk.equals(\""I/F적재형\"")) {
                                                                      ifTableCount++;
                                                                      continue;
                                                        }

String sUSER \u003d \""\"";

if (sub.equals(\""DOP\"") || sub.equals(\""DEC\"") || sub.equals(\""DPF\"")) {
sUSER \u003d \""810257 조광희\""

String UCDfilePath0010 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\\""+ sub + \""\\\\\"" + UCD + \""_UN_0010.sql\"";
String UIDfilePath0010 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\INI\\\\\""+ sub + \""\\\\\"" + UID + \""_UN_0010.sql\"";
String LCDSNPfilePath0010 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\\""+ sub + \""\\\\\"" + LCD + \""_US_0010.sql\"";
String LCDSNPfilePath0020 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\\""+ sub + \""\\\\\"" + LCD + \""_US_0020.sql\"";
String LCDSNPfilePath0030 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\\""+ sub + \""\\\\\"" + LCD + \""_US_0030.sql\"";
String LCDfilePath0010 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\\""+ sub + \""\\\\\"" + LCD + \""_US_0010.sql\"";
String LCDfilePath0020 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\\""+ sub + \""\\\\\"" + LCD + \""_LD_0020.sql\"";
String LCDfilePath0030 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\\""+ sub + \""\\\\\"" + LCD + \""_US_0030.sql\"";
String LCDfilePath0040 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\\""+ sub + \""\\\\\"" + LCD + \""_US_0040.sql\"";
String LIDfilePath0010 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\INI\\\\\""+ sub + \""\\\\\"" + LID + \""_US_0010.sql\"";
String LIDfilePath0020 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\INI\\\\\""+ sub + \""\\\\\"" + LID + \""_LD_0020.sql\"";
String LIDfilePath0030 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\INI\\\\\""+ sub + \""\\\\\"" + LID + \""_US_0030.sql\"";
String LIDfilePath0040 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\INI\\\\\""+ sub + \""\\\\\"" + LID + \""_US_0040.sql\"";


if ( tblsnps.equals(\""Y\"")) {
String tblowner \u003d \""SOROWN\"";
String tblPartnm \u003d tblID + \""_PTR$$[1]\"";

String sqlSnp0010Trc \u003d \""COMMIT;\\r\
\""
+ \""\\r\
\""
+ \""DECLARE\\r\
\""
+ \""          P_RTN                                          VARCHAR2(1000);\\r\
\""
+ \""BEGIN\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION SET NLS_DATE_FROMAT \u003d \\\\\\\""YYYYMMDD\\\\\\\""\u0027;\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION SET NLS_TIMESTAMP_FORMAT \u003d \\\\\\\""YYYYMMDDHH24MISS\\\\\\\""\u0027;\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION ENABLE PARALLEL DML\u0027;\\r\
\""
+ \""\\r\
\""
+ \""            SP_ADW_TRUNCATE(P_RTN,\u0027\""+tblowner + \""\u0027,\u0027\"" + tblID + \""\u0027,\u0027\"" + tblPartnm + \""\u0027);\\r\
\""
+ \""\\r\
\""
+ \""            IF P_RTN \u003c\u003e \u00270\u0027 THEN\\r\
\""
+ \""                          O_RESULT :\u003d 1;\\r\
\""
+ \""                          O_ERRMSG :\u003d \u0027TRUNCATE ERROR\u0027;\\r\
\""
+ \""            END IF;\\r\
\""
+ \""\\r\
\""
+ \""            COMMIT;\\r\
\""
+ \""END;\\r\
\""

File LCDSNPfile0010 \u003d new File(LCDSNPfilePath0010);

if (!LCDSNPfile0010.exists()) {

FileOutputStream fileOutStream \u003d new FileOutputStream(LCDSNPfilePath0010);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlSnp0010Trc);
bufferedWriter.close();
changeCount ++;
} else {
FileInputStream fileInStream \u003d new FileInputStream(LCDSNPfilePath0010);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;
String LCDSnpFile0010OldFile \u003d \""\"";

while((ch \u003d bufferedReader.read()) !\u003d -1) {

LCDSnpFile0010OldFile +\u003d (char)ch;
}

if (LCDSnpFile0010OldFile.equals(sqlSnp0010Trc)) {

} else if (!LCDSnpFile0010OldFile.equals(sqlSnp0010Trc) \u0026\u0026 LCDSNPfile0010.exists()) {
FileOutputStream fileOutStream \u003d new FileOutputStream(LCDSNPfilePath0010);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlSnp0010Trc);
bufferedWriter.close();
changeCount ++;
}
}
}

if (tblsnps.equals(\""Y\"")) {
String tblowner \u003d \""SOROWN\"";
String srcTble \u003d \""D\"" + cbstbl.getStringCellValue().substring(1);

sorrs \u003d pstmsor.executeQuery();

String columnlist \u003d \""\"";
String colsqllist \u003d \""\"";

 while (sorrs.next()) {
                                                                      String sorowner \u003d sorrs.getString(\""OWNER\"");
                                                                      String sortblnm \u003d sorrs.getString(\""TABLE_NAME\"");
                                                                      String sortblco \u003d sorrs.getString(\""TABLE_COMMENTS\"");
                                                                      String sorcolumn \u003d sorrs.getString(\""COLUMN_NAME\"");
                                                                      String sorcolumnco \u003d sorrs.getString(\""COLUMN_COMMENTS\"");
                                                                      String sortype \u003d sorrs.getString(\""DATA_TYPE\"");
                                                                      String sorlength \u003d sorrs.getString(\""DATA_LENGTH\"");
                                                                      String sorprecision \u003d sorrs.getString(\""DATA_PRECISION\"");
                                                                      String sorscale \u003d sorrs.getString(\""DATA_SCALE\"");
                                                                      String sornull \u003d sorrs.getString(\""NULLABLE\"");
                                                                      int sorid \u003d sorrs.getInt(\""COLUMN_ID\"");
                                                                      String sordefaultlen \u003d sorrs.getString(\""DEFAULT_LENGTH\"");
                                                                      String sordefault \u003d sorrs.getString(\""DATA_DEFAULT\"").trim();
                                                                      String sorpk \u003d sorrs.getString(\""PK_YN\"");
                                                                      String sorcollen \u003d sorrs.getString(\""MAX_COL_LEN\"");
                                                                      String sormaxcollen \u003d sorrs.getString(\""PK_MAX_COL_LEN\"");

String collist \u003d null;
if(sorid \u003d\u003d 1) {
collist \u003d \""                  (\""+sorcolumn;
} else {
collist \u003d \""                   ,\""+sorcolumn;
}
columnlist +\u003d collist + \""\\r\
\"";

String colsql \u003d \""                 \"";
if(sorid \u003d\u003d 1) {
colsql +\u003d \"" \"";
} else {
colsql +\u003d \"",\"";
}
if(sorcolumn.equals(\""SNPSH_BASE_DT\"")) {
colsql +\u003d \""\u0027$$[1]\u0027\"";
} else if(sorcolumn.equals(\""DW_LDNG_DTTM\"")) {
colsql +\u003d \""TO_CHAR(SYSTIMESTAMP,\u0027YYYYMMDDHH24MISSFF2\u0027)\"";
} else {
colsql +\u003d sorcolumn;
}
colsqllist +\u003d colsql + \""\\r\
\"";
}

String sqlSnp0020Trc \u003d \""COMMIT;\\r\
\""
+ \""\\r\
\""
+ \""DECLARE\\r\
\""
+ \""          P_RTN                                          VARCHAR2(1000);\\r\
\""
+ \""BEGIN\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION SET NLS_DATE_FROMAT \u003d \\\\\\\""YYYYMMDD\\\\\\\""\u0027;\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION SET NLS_TIMESTAMP_FORMAT \u003d \\\\\\\""YYYYMMDDHH24MISS\\\\\\\""\u0027;\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION ENABLE PARALLEL DML\u0027;\\r\
\""
+ \""\\r\
\""
+ \""            INSERT /* \"" + sUSER +\"" /ADW/CHG/\"" + sub + \""/\"" + LCD + \"" - US_0020 */\\r\
\""
+ \""                             /*+ APPEND PARALLEL(4) */ \\r\
\""
+ \""                  INTO SOROWN.\"" + tblID + \""\\r\
\""
+ columnlist
+ \""                      )\\r\
\""
+ \""SELECT /*+ FULL(T10) PARALLEL(4) */\\r\
\""
+ colsqllist
+ \""       FROM SOROWN.\""+ srcTbl + \"" T10\\r\
\""
+ \""       ;\\r\
\""
+ \""\\r\
\""
+ \""        O_COUNT :\u003d SQL%ROWCOUNT;\\r\
\""
+ \""        O_ERRMSG :\u003d SQLERRM;\\r\
\""
+ \""\\r\
\""
+ \""        COMMIT;\\r\
\""
+ \""END;\\r\
\"";

File LCDSNPfile0020 \u003d new File(LCDSNPfilePath0020);

if (!LCDSNPfile0020.exists()) {

FileOutputStream fileOutStream \u003d new FileOutputStream(LCDSNPfilePath0020);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlSnp0020Trc);
bufferedWriter.close();
changeCount ++;
} else {
FileInputStream fileInStream \u003d new FileInputStream(LCDSNPfilePath0020);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;
String LCDSnpFile0020OldFile \u003d \""\"";

while((ch \u003d bufferedReader.read()) !\u003d -1) {

LCDSnpFile0020OldFile +\u003d (char)ch;
}

if (LCDSnpFile0020OldFile(sqlSnp0020Trc)) {

} else if (!LCDSnpFile0020OldFile.equals(sqlSnp0020Trc) \u0026\u0026 LCDSNPfile0020.exists()) {
FileOutputStream fileOutStream \u003d new FileOutputStream(LCDSNPfilePath0020);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlSnp0020Trc);
bufferedWriter.close();
changeCount ++;
}
}


if (tblsnps.equals(\""Y\"")) {
String owner \u003d \""SOROWN\"";
String tblPartnm \u003d tblID + \""_PTR$$[1]\"";

String sqlSnp0030Trc \u003d \""COMMIT;\\r\
\""
+ \""\\r\
\""
+ \""DECLARE\\r\
\""
+ \""          P_RTN                                          VARCHAR2(1000);\\r\
\""
+ \""BEGIN\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION SET NLS_DATE_FROMAT \u003d \\\\\\\""YYYYMMDD\\\\\\\""\u0027;\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION SET NLS_TIMESTAMP_FORMAT \u003d \\\\\\\""YYYYMMDDHH24MISS\\\\\\\""\u0027;\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION ENABLE PARALLEL DML\u0027;\\r\
\""
+ \""\\r\
\""
+ \""           SP_ADW_STATS_GATHER (P_RTN, \u0027\"" + owner +\""\u0027, \"" +\""\u0027\"" +tblID + \""\u0027, \"" + \""\u0027\"" + tblPartnm + \""\u0027);\\r\
\""
+ \""\\r\
\""
+ \""          COMMIT;\\r\
\""
+ \""END;\\r\
\""


File LCDSNPfile0030 \u003d new File(LCDSNPfilePath0030);

if (!LCDSNPfile0030.exists()) {

FileOutputStream fileOutStream \u003d new FileOutputStream(LCDSNPfilePath0030);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlSnp0030Trc);
bufferedWriter.close();
changeCount ++;
} else {
FileInputStream fileInStream \u003d new FileInputStream(LCDSNPfilePath0030);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;
String LCDSnpFile0030OldFile \u003d \""\"";

while((ch \u003d bufferedReader.read()) !\u003d -1) {

LCDSnpFile0030OldFile +\u003d (char)ch;
}

if (LCDSnpFile0030OldFile(sqlSnp0030Trc)) {

} else if (!LCDSnpFile0030OldFile.equals(sqlSnp0030Trc) \u0026\u0026 LCDSNPfile0030.exists()) {
FileOutputStream fileOutStream \u003d new FileOutputStream(LCDSNPfilePath0030);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlSnp0030Trc);
bufferedWriter.close();
changeCount ++;
}
}
}
}
if(!UCD.equals(null) \u0026\u0026 !tblsnps.equals(\""Y\"")) {
cbsrs \u003d pstmcbs.executeQuery();

String UIDsql \u003d \""\"";
String UCDsql \u003d \""\"";
String tblOwner \u003d \""\"";
String tblTblnm \u003d \""\"";

                                                        while (cbsrs.next()) {
                                                                      String cbsowner \u003d cbsRs.getString(\""OWNER\"");
                                                                      String cbstblnm \u003d cbsRs.getString(\""TABLE_NAME\"");
                                                                      String cbstblco \u003d cbsRs.getString(\""TABLE_COMMENTS\"");
                                                                      String cbscolumn \u003d cbsRs.getString(\""COLUMN_NAME\"");
                                                                      String cbscolumnco \u003d cbsRs.getString(\""COLUMN_COMMENTS\"");
                                                                      String cbstype \u003d cbsRs.getString(\""DATA_TYPE\"");
                                                                      String cbslength \u003d cbsRs.getString(\""DATA_LENGTH\"");
                                                                      String cbsprecision \u003d cbsRs.getString(\""DATA_PRECISION\"");
                                                                      String cbsscale \u003d cbsRs.getString(\""DATA_SCALE\"");
                                                                      String cbsnull \u003d cbsRs.getString(\""NULLABLE\"");
                                                                      int cbsId \u003d cbsRs.getInt(\""COLUMN_ID\"");
                                                                      String cbsdefaultLen \u003d cbsRs.getString(\""DEFAULT_LENGTH\"");
                                                                      String cbsdefault \u003d cbsRs.getString(\""DATA_DEFAULT\"").trim();
                                                                      String cbspk \u003d cbsRs.getString(\""PK_YN\"");

String U0010sql \u003d \""\"";
tblOwner \u003d cbsowner;
tblTblnm \u003d cbstblnm;

if(cbsid \u003d\u003d 1) {
U0010sql +\u003d \""             \"" + cbscolumn;
} else if(cbscolumn.equals(\""RLNM_DVN_NO\"")) {
U0010sql +\u003d \""            ,TRIM(REPLACE(\"" + cbscolumn + \"", \u0027-\u0027, \u0027\u0027) AS \"" + cbscolumn;
}
UCDsql +\u003d U0010sql + \""\\r\
\"";
UIDsql +\u003d U0010sql + \""\\r\
\"";
}


String sqlUID0010Trc \u003d \""SELECT /* \"" + sUSER + \"" /ADW/INI/\"" + sub + \""/\"" + UID + \"" - UN_0010 */\\r\
\""
+ UIDsql
+ \""    FROM \"" + tblOwner + \"".\"" + tblTblnm + \""\\r\
\"";

File UIDfile0010 \u003d new File(UIDfilePath0010);

if (!UIDfile0010.exists()) {

FileOutputStream fileOutStream \u003d new FileOutputStream(UIDfilePath0010);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlUID0010Trc);
bufferedWriter.close();
changeCount ++;
} else {
FileInputStream fileInStream \u003d new FileInputStream(UIDfilePath0010);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;
String UIDFile0010OldFile \u003d \""\"";

while((ch \u003d bufferedReader.read()) !\u003d -1) {

UIDFile0010OldFile +\u003d (char)ch;
}

if (UIDFile0010OldFile(sqlUID0010Trc)) {

} else if (!UIDFile0010OldFile(sqlUID0010Trc) \u0026\u0026 UIDfile0010.exists()) {
FileOutputStream fileOutStream \u003d new FileOutputStream(UIDfilePath0010);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlUID0010Trc);
bufferedWriter.close();
changeCount ++;
}
}

String sqlUCD0010Trc \u003d \""\"";

if(loadchk.equals(\""변경적재형\"") \u0026\u0026 !UCD.equals(null)) {

sqlUCD0010Trc \u003d \""SELECT /* \"" + sUSER + \"" /ADW/CHG/\"" + sub + \""/\"" + UCD + \"" - UN_0010 */\\r\
\""
+ UCDsql
+ \""    FROM \"" + tblOwner + \"".\"" + tblTblnm + \""\\r\
\""
+ \"" WHERE SYS_LAST_CHNG_DTTM \u003e\u003d \u0027$$[1]\u0027||\u002700000000\u0027\\r\
\""
+         OR FRMW_CHNG_TMST \u003e\u003d TO_TIMESTAMP(\u0027$$[1]\u0027||\u0027000000\u0027, \u0027YYYYMMDDHH24MISS\u0027)\\r\
\"";

} else if(loadchk.equals(\""초기적재형\"") \u0026\u0026 !UCD.eqlals(null)) {

String sqlUCD0010Trc \u003d \""SELECT /* \"" + sUSER + \"" /ADW/CHG/\"" + sub + \""/\"" + UCD + \"" - UN_0010 */\\r\
\""
+ UCDsql
+ \""    FROM \"" + tblOwner + \"".\"" + tblTblnm + \""\\r\
\"";

File UCDfile0010 \u003d new File(UCDfilePath0010);

if (!UCDfile0010.exists()) {

FileOutputStream fileOutStream \u003d new FileOutputStream(UCDfilePath0010);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlUCD0010Trc);
bufferedWriter.close();
changeCount ++;
} else {
FileInputStream fileInStream \u003d new FileInputStream(UCDfilePath0010);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;
String UCDFile0010OldFile \u003d \""\"";

while((ch \u003d bufferedReader.read()) !\u003d -1) {

UCDFile0010OldFile +\u003d (char)ch;
}

if (UCDFile0010OldFile(sqlUCD0010Trc)) {

} else if (!UCDFile0010OldFile(sqlUCD0010Trc) \u0026\u0026 UCDfile0010.exists()) {
FileOutputStream fileOutStream \u003d new FileOutputStream(UCDfilePath0010);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlUCD0010Trc);
bufferedWriter.close();
changeCount ++;
}
}
"
"}

if(!LCD.equals(null) \u0026\u0026 !tblsnps.equals(\""Y\"")) {

String tblOwner \u003d \""STGOWN\"";
String tblPartnm \u003d \""\"";

String sqlLD0010Trc \u003d \""COMMIT;\\r\
\""
+ \""\\r\
\""
+ \""DECLARE\\r\
\""
+ \""          P_RTN                                          VARCHAR2(1000);\\r\
\""
+ \""BEGIN\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION SET NLS_DATE_FROMAT \u003d \\\\\\\""YYYYMMDD\\\\\\\""\u0027;\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION SET NLS_TIMESTAMP_FORMAT \u003d \\\\\\\""YYYYMMDDHH24MISS\\\\\\\""\u0027;\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION ENABLE PARALLEL DML\u0027;\\r\
\""
+ \""\\r\
\""
+ \""           SP_ADW_TRUNCATE(P_RTN, \u0027\"" + tblOwner +\""\u0027, \"" +\""\u0027\"" +tblID + \""\u0027, \"" + \""\u0027\"" + tblPartnm + \""\u0027);\\r\
\""
+ \""\\r\
\""
+ \""           IF P_RTN \u003c\u003e \u00270\u0027 THEN\\r\
\""
+ \""                   O_RESULT :\u003d 1;\\r\
\""
+ \""                   O_ERRMSG :\u003d \u0027TRUNCATE ERROR\u0027;\\r\
\""
+ \""             END IF;\\r\
\""
+ \""\\r\
\""
+ \""           COMMIT;\\r\
\""
+ \""END;\\r\
\""

File LIDfile0010 \u003d new File(LIDfilePath0010);

if (!LIDfile0010.exists()) {

FileOutputStream fileOutStream \u003d new FileOutputStream(LIDfilePath0010);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlLD0010Trc);
bufferedWriter.close();
changeCount ++;
} else {
FileInputStream fileInStream \u003d new FileInputStream(LIDfilePath0010);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;
String LIDFile0010OldFile \u003d \""\"";

while((ch \u003d bufferedReader.read()) !\u003d -1) {

LIDFile0010OldFile +\u003d (char)ch;
}

if (LIDFile0010OldFile(sqlLD0010Trc)) {

} else if (!LIDFile0010OldFile(sqlLD0010Trc) \u0026\u0026 LIDfile0010.exists()) {
FileOutputStream fileOutStream \u003d new FileOutputStream(LIDfilePath0010);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlLD0010Trc);
bufferedWriter.close();
changeCount ++;
}
}

File LCDfile0010 \u003d new File(LCDfilePath0010);

if (!LCDfile0010.exists()) {

FileOutputStream fileOutStream \u003d new FileOutputStream(LCDfilePath0010);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlLD0010Trc);
bufferedWriter.close();
changeCount ++;
} else {
FileInputStream fileInStream \u003d new FileInputStream(LCDfilePath0010);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;
String LCDFile0010OldFile \u003d \""\"";

while((ch \u003d bufferedReader.read()) !\u003d -1) {

LCDFile0010OldFile +\u003d (char)ch;
}

if (LCDFile0010OldFile(sqlLD0010Trc)) {

} else if (!LCDFile0010OldFile(sqlLD0010Trc) \u0026\u0026 LCDfile0010.exists()) {
FileOutputStream fileOutStream \u003d new FileOutputStream(LCDfilePath0010);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlLD0010Trc);
bufferedWriter.close();
changeCount ++;
}
}
}


if(!LCD.equals(null) \u0026\u0026 !tblsnps.equals(\""Y\"")) {
cbsrs \u003d pstmcbs.executeQuery();

String tblOwner \u003d \""STGOWN\"";
String colInfos \u003d \""\"";

                                                        while (cbsrs.next()) {
                                                                      String cbsowner \u003d cbsRs.getString(\""OWNER\"");
                                                                      String cbstblnm \u003d cbsRs.getString(\""TABLE_NAME\"");
                                                                      String cbstblco \u003d cbsRs.getString(\""TABLE_COMMENTS\"");
                                                                      String cbscolumn \u003d cbsRs.getString(\""COLUMN_NAME\"");
                                                                      String cbscolumnco \u003d cbsRs.getString(\""COLUMN_COMMENTS\"");
                                                                      String cbstype \u003d cbsRs.getString(\""DATA_TYPE\"");
                                                                      String cbslength \u003d cbsRs.getString(\""DATA_LENGTH\"");
                                                                      String cbsprecision \u003d cbsRs.getString(\""DATA_PRECISION\"");
                                                                      String cbsscale \u003d cbsRs.getString(\""DATA_SCALE\"");
                                                                      String cbsnull \u003d cbsRs.getString(\""NULLABLE\"");
                                                                      int cbsId \u003d cbsRs.getInt(\""COLUMN_ID\"");
                                                                      String cbsdefaultLen \u003d cbsRs.getString(\""DEFAULT_LENGTH\"");
                                                                      String cbsdefault \u003d cbsRs.getString(\""DATA_DEFAULT\"").trim();
                                                                      String cbspk \u003d cbsRs.getString(\""PK_YN\"");

String colInfo \u003d \""\"";
String colType \u003d \""\"";

if (cbsid \u003d\u003d 1) {
colInfo +\u003d \""                \"" + rPad.padRight(cbscolumn, 32, \u0027 \u0027);
} else {
colInfo +\u003d \""              ,\"" + rPad.padRight(cbscolumn, 32, \u0027 \u0027);
}

if (cbstype.contains(\""TIMESTAMP\"")) {
colType +\u003d \""TIMESTAMP \\\\\\\""YYYYMMDDHH@$MISSFF6\\\\\\\""\"";
} else if (cbstype.contains(\""VARCHAR\"")) {
colType \u003d \""CHAR\"";
if(cbslength \u003e 250) {
colType +\u003d \""(\"" + cbslength + \"")\"";
}
} else if (cbstype.contains(\""NUMBER\"") \u0026\u0026 cbsscale \u003d\u003d null) {
colType \u003d \""INTEGER EXTERNAL\"";
} else if (cbstype.contains(\""NUMBER\"")) {
colType \u003d \""DECIMAL EXTERNAL\"";
}
colInfos +\u003d colInfo + colType + \""\\r\
\"";
}

String sqlLID0020Trc \u003d \""LOAD DATA\\r\
\""
+ \""INFILE \u0027$$[SOR_INI_SAM]/\"" + sub +  \""/\"" + tblID + \"".dat\u0027 \\\\\\\""STR \u0027|^,\\\\\\\
\u0027\\\\\\\""\\r\
\""
+ \""APPEND\\r\
\""
+ \""INTO TABLE \"" + tblOwner + \"".\"" + tblID + \""\\r\
\""
+ \""FIELDS TERMINATED BY \u0027|^,\u0027\\r\
\""
+ \""TRAILING NULLCOLS\\r\
\""
+ \""(\\r\
\""
+ colInfos
+ \"")\\r\
\"";
String sqlLCD0020Trc \u003d \""LOAD DATA\\r\
\""
+ \""INFILE \u0027$$[SOR_CHG_SAM]/\"" + sub +  \""/\"" + tblID + \"".dat\u0027 \\\\\\\""STR \u0027|^,\\\\\\\
\u0027\\\\\\\""\\r\
\""
+ \""APPEND\\r\
\""
+ \""INTO TABLE \"" + tblOwner + \"".\"" + tblID + \""\\r\
\""
+ \""FIELDS TERMINATED BY \u0027|^,\u0027\\r\
\""
+ \""TRAILING NULLCOLS\\r\
\""
+ \""(\\r\
\""
+ colInfos
+ \"")\\r\
\"";

File LIDfile0020 \u003d new File(LIDfilePath0020);

if (!LIDfile0020.exists()) {

FileOutputStream fileOutStream \u003d new FileOutputStream(LIDfilePath0020);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlLID0020Trc);
bufferedWriter.close();
changeCount ++;
} else {
FileInputStream fileInStream \u003d new FileInputStream(LIDfilePath0020);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;
String LIDFile0020OldFile \u003d \""\"";

while((ch \u003d bufferedReader.read()) !\u003d -1) {

LIDFile0020OldFile +\u003d (char)ch;
}

if (LIDFile0020OldFile(sqlLID0020Trc)) {

} else if (!LIDFile0020OldFile(sqlLID0020Trc) \u0026\u0026 LIDfile0020.exists()) {
FileOutputStream fileOutStream \u003d new FileOutputStream(LIDfilePath0020);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlLID0020Trc);
bufferedWriter.close();
changeCount ++;
}
}

File LCDfile0020 \u003d new File(LCDfilePath0020);

if (!LCDfile0020.exists()) {

FileOutputStream fileOutStream \u003d new FileOutputStream(LCDfilePath0020);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlLCD0020Trc);
bufferedWriter.close();
changeCount ++;
} else {
FileInputStream fileInStream \u003d new FileInputStream(LCDfilePath0020);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;
String LCDFile0020OldFile \u003d \""\"";

while((ch \u003d bufferedReader.read()) !\u003d -1) {

LCDFile0020OldFile +\u003d (char)ch;
}

if (LCDFile0020OldFile(sqlLCD0020Trc)) {

} else if (!LCDFile0020OldFile(sqlLCD0020Trc) \u0026\u0026 LCDfile0020.exists()) {
FileOutputStream fileOutStream \u003d new FileOutputStream(LCDfilePath0020);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlLCD0020Trc);
bufferedWriter.close();
changeCount ++;
}
}
}

if (!LID.equals(null) \u0026\u0026 !tblsnps.equals(\""Y\"")) {

String tblOwner \u003d \""SOROWN\"";
String Partnm \u003d \""\"";
String sqlLID0030Trc \u003d \""\"";

if (!snpschk.equals(\""SNPSH_BASE_DT\"") \u0026\u0026 !snpschk.equals(null)) {
String sqlPartTrc \u003d \""COMMIT;\\r\
\""
+ \""\\r\
\""
+ \""DECLARE\\r\
\""
+ \""      P_RTN              VARCHAR2(1000);\\r\
\""
+ \""      V_OWNER      VARCHAR2(20) :\u003d \u0027\"" + tblOwner + \""\u0027;\\r\
\""
+ \""      V_TABLE         VARCHAR2(20) :\u003d \u0027\"" + tblID + \""\u0027;\\r\
\""
+ \""      V_PART_YN   VARCHAR2(1);\\r\
\""
+ \""      V_CNT               NUMBER(10);\\r\
\""
+ \""BEGIN\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION SET NLS_DATE_FROMAT \u003d \\\\\\\""YYYYMMDD\\\\\\\""\u0027;\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION SET NLS_TIMESTAMP_FORMAT \u003d \\\\\\\""YYYYMMDDHH24MISS\\\\\\\""\u0027;\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION ENABLE PARALLEL DML\u0027;\\r\
\""
+ \""\\r\
\""
+ \""    SELECT \\r\
\""
+ \""         NVL(MAX(CASE WHEN PARTITIONED \u003d \u0027YES\u0027 THEN \u0027Y\u0027 ELSE \u0027N\u0027 END), \u0027X\u0027) AS PART_YN\\r\
\""
+ \""        INTO V_PART_YN        \\r\
\""
+ \""        FROM ALL_TABLES\\r\
\""
+ \""      WHERE OWNER \u003d V_OWNER\\r\
\""
+ \""           AND TABLE_NAME \u003d V_TABLE\\r\
\""
+ \""        ;\\r\
\""
+ \""\\r\
\""
+ \""          DBMS_OUTPUT.PUT_LINE(V_OWNER ||\u0027.\u0027|| V_TABLE || \u0027 PART_YN : [\u0027||V_PART_YN||\u0027]\u0027);\\r\
\""
+ \""          \\r\
\""
+ \""          IF V_PART_YN \u003d \u0027N\u0027 THEN\\r\
\""
+ \""                  SP_ADW_TRUNCATE(P_RTN,V_OWNER,V_TABLE,\u0027\u0027);\\r\
\""
+ \""\\r\
\""
+ \""          IF P_RTN \u003c\u003e \u00270\u0027 THEN \u003d\\r\
\""
+ \""               O_RESULT :\u003d 1;\\r\
\""
+ \""               O_ERRMSG :\u003d \u0027TRUNCATE ERROR\u0027;\\r\
\""
+ \""          END IF;\\r\
\""
+ \""      ELSIF V_PART_YN \u003d \u0027Y\u0027 THEN\\r\
\""
+ \""               FOR PTR IN\\r\
\""
+ \""             (\\r\
\""
+ \""                      SELECT \\r\
\""
+ \""                                  TABLE_OWNER\\r\
\""
+ \""                                  ,TABLE_NAME \\r\
\""
+ \""                                 ,PARTITION_NAME\\r\
\""
+ \""                            FROM ALL_TAB_PARTITIONS\\r\
\""
+ \""                       WHERE TABLE_OWNER \u003d V_OWNER\\r\
\""
+ \""                           AND TABLE_NAME \u003d V_TABLE\\r\
\""
+ \""                  )\\r\
\""
+ \""                LOOP\\r\
\""
+ \""                          EXECUTE IMMEDIATE \u0027SELECT COUNT(1) FROM \u0027 || PTR.TABLE_OWNER || \u0027.\u0027 || PTR.TABLE_NAME || \u0027 PARTITION(\u0027||PTR.PARTITION_NAME||\u0027)\u0027 INTO V_CNT;\\r\
\""
+ \""\\r\
\""
+ \""                 IF V_CNT \u003e 0 THEN\\r\
\""
+ \""                           DBMS_OUTPUT.PUT_LINE(PTR.PARTITION_NAME|| \u0027 Partition Data Count : \u0027||V_CNT);\\r\
\""
+ \""                 \\r\
\""
+ \""                            IF P_RTN \u003c\u003e \u00270\u0027 THEN\\r\
\""
+ \""                                      O_RESULT :\u003d 1;\\r\
\""
+ \""                                      O_ERRMSG :\u003d \u0027PARTITION TRUNCATE ERROR\u0027;\\r\
\""
+ \""                                      EXIT;\\r\
\""
+ \""                           END IF;\\r\
\""
+ \""                 END IF;\\r\
\""
+ \""        END LOOP;\\r\
\""
+ \""      ELSE\\r\
\""
+ \""                O_RESULT :\u003d 1;\\r\
\""
+ \""                 O_ERRMSG :\u003d \u0027TABLE NOT EXIST ERROR\u0027;\\r\
\""
+ \""        END IF;\\r\
\""
+ \""\\r\
\""
+ \""           COMMIT;\\r\
\""
+ \""END;\\r\
\"";

sqlLID0030Trc +\u003d sqlPartTrc + \""\\r\
\"";

if (loadchk.equals(\""초기적재형\"")) {
}
}

File LIDfile0030 \u003d new File(LIDfilePath0030);

if (!LIDfile0030.exists()) {

FileOutputStream fileOutStream \u003d new FileOutputStream(LIDfilePath0030);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlLID0030Trc);
bufferedWriter.close();
changeCount ++;
} else {
FileInputStream fileInStream \u003d new FileInputStream(LIDfilePath0030);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;
String LIDFile0030OldFile \u003d \""\"";

while((ch \u003d bufferedReader.read()) !\u003d -1) {

LIDFile0030OldFile +\u003d (char)ch;
}

if (LIDFile0030OldFile(sqlLID0030Trc)) {

} else if (!LIDFile0030OldFile(sqlLID0030Trc) \u0026\u0026 LIDfile0030.exists()) {
FileOutputStream fileOutStream \u003d new FileOutputStream(LIDfilePath0030);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlLID0030Trc);
bufferedWriter.close();
changeCount ++;
}
}

File LCDfile0030 \u003d new File(LCDfilePath0030);

if (!LCDfile0030.exists() \u0026\u0026 loadchk.equals(\""초기적재형\"")) {

FileOutputStream fileOutStream \u003d new FileOutputStream(LCDfilePath0030);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlLCD0030Trc);
bufferedWriter.close();
changeCount ++;
} else if (loadchk.equals(\""초기적재형\"")){
FileInputStream fileInStream \u003d new FileInputStream(LCDfilePath0030);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;
String LCDFile0030OldFile \u003d \""\"";

while((ch \u003d bufferedReader.read()) !\u003d -1) {

LCDFile0030OldFile +\u003d (char)ch;
}

if (LCDFile0030OldFile(sqlLCD0030Trc) \u0026\u0026 loadchk.equals(\""초기적재형\"")) {

} else if (!LCDFile0030OldFile(sqlLCD0030Trc) \u0026\u0026 LCDfile0030.exists() \u0026\u0026 loadchk.equals(\""초기적재형\"")) {
FileOutputStream fileOutStream \u003d new FileOutputStream(LCDfilePath0030);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlLCD0030Trc);
bufferedWriter.close();
changeCount ++;
}
}
}

if (!LCD.equals(null) \u0026\u0026 !tblsnps.equals(\""Y\"") \u0026\u0026 loadchk.equals.equals(\""변경적재형\"")) {
sorrs \u003d pstmsor.executeQuery();

String tblOwner \u003d \""SOROWN\"";
String Partnm \u003d \""\"";

String PKchk \u003d \""\"";

 while (sorrs.next()) {
                                                                      String sorowner \u003d sorrs.getString(\""OWNER\"");
                                                                      String sortblnm \u003d sorrs.getString(\""TABLE_NAME\"");
                                                                      String sortblco \u003d sorrs.getString(\""TABLE_COMMENTS\"");
                                                                      String sorcolumn \u003d sorrs.getString(\""COLUMN_NAME\"");
                                                                      String sorcolumnco \u003d sorrs.getString(\""COLUMN_COMMENTS\"");
                                                                      String sortype \u003d sorrs.getString(\""DATA_TYPE\"");
                                                                      String sorlength \u003d sorrs.getString(\""DATA_LENGTH\"");
                                                                      String sorprecision \u003d sorrs.getString(\""DATA_PRECISION\"");
                                                                      String sorscale \u003d sorrs.getString(\""DATA_SCALE\"");
                                                                      String sornull \u003d sorrs.getString(\""NULLABLE\"");
                                                                      int sorid \u003d sorrs.getInt(\""COLUMN_ID\"");
                                                                      String sordefaultlen \u003d sorrs.getString(\""DEFAULT_LENGTH\"");
                                                                      String sordefault \u003d sorrs.getString(\""DATA_DEFAULT\"").trim();
                                                                      String sorpk \u003d sorrs.getString(\""PK_YN\"");
                                                                      String sorcollen \u003d sorrs.getString(\""MAX_COL_LEN\"");
                                                                      String sormaxcollen \u003d sorrs.getString(\""PK_MAX_COL_LEN\"");

String PK \u003d \""\"";

if (sorpk.equals(\""Y\"")) {
if (sorid \u003d\u003d 1) {
PK +\u003d \""                         WHERE T11.\"" + rPad.padRight(sorcolumn, sormaxcollen, \u0027 \u0027) + \"" \u003d T10.\"" + sorcolumn + \""\\r\
\"";
} else {
PK +\u003d \""                               AND T11.\"" + rPad.padRight(sorcolumn, sormaxcollen, \u0027 \u0027) + \"" \u003d T10.\"" + sorcolumn + \""\\r\
\"";
}
}
PKchk +\u003d PK;
}

String sqlLCD0030Trc \u003d \""COMMIT;\\r\
\""
+ \""\\r\
\""
+ \""DECLARE\\r\
\""
+ \""      P_RTN              VARCHAR2(1000);\\r\
\""
+ \""BEGIN\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION SET NLS_DATE_FROMAT \u003d \\\\\\\""YYYYMMDD\\\\\\\""\u0027;\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION SET NLS_TIMESTAMP_FORMAT \u003d \\\\\\\""YYYYMMDDHH24MISS\\\\\\\""\u0027;\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION ENABLE PARALLEL DML\u0027;\\r\
\""
+ \""\\r\
\""
+ \""          DELETE /* \"" + sUSER + \""  /ADW/CHG/\"" + sub + \""/\"" + LCD + \"" - US_0030 */\\r\
\""
+ \""                            /*+ USE_HASH(T10 T11) PARALLEL(4) PQ_DISTRIBUTE(T10 HASH HASH) PQ_DISTRIBUTE(T11 HASH HASH) */ \\r\
\""
+ \""                FROM SOROWN.\"" + tblID + \"" T10\\r\
\""
+ \""             WHERE EXISTS (\\r\
\""
+ \""                                                   SELECT \u0027X\u0027\\r\
\""
+ \""                                                         FROM STGOWN.\"" + tblID + \"" T11\\r\
\""
+ PKchk
+ \""                                                )\\r\
\""
+ \""                ;\\r\
\""
+ \""\\r\
\""
+ \""            O_COUNT :\u003d SQL%ROWCOUNT;\\r\
\""
+ \""            O_ERRMSG :\u003d SQLERRM;\\r\
\""
+ \""\\r\
\""
+ \""           COMMIT;\\r\
\""
+ \""END;\\r\
\""

File LCDfile0030 \u003d new File(LCDfilePath0030);

if (!LCDfile0030.exists()) {

FileOutputStream fileOutStream \u003d new FileOutputStream(LCDfilePath0030);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlLCD0030Trc);
bufferedWriter.close();
changeCount ++;
} else {
FileInputStream fileInStream \u003d new FileInputStream(LCDfilePath0030);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;
String LCDFile0030OldFile \u003d \""\"";

while((ch \u003d bufferedReader.read()) !\u003d -1) {

LCDFile0030OldFile +\u003d (char)ch;
}

if (LCDFile0030OldFile(sqlLCD0030Trc)) {

} else if (!LCDFile0030OldFile(sqlLCD0030Trc) \u0026\u0026 LCDfile0030.exists()) {
FileOutputStream fileOutStream \u003d new FileOutputStream(LCDfilePath0030);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlLCD0030Trc);
bufferedWriter.close();
changeCount ++;
}
}
}

if (!LCD.equals(null) \u0026\u0026 !tblsnps.equals(\""Y\"")) {
sorrs \u003d pstmsor.executeQuery();

String tblOwner \u003d \""SOROWN\"";
String Insertsql \u003d \""\"";
String Selectsql \u003d \""\"";

 while (sorrs.next()) {
                                                                      String sorowner \u003d sorrs.getString(\""OWNER\"");
                                                                      String sortblnm \u003d sorrs.getString(\""TABLE_NAME\"");
                                                                      String sortblco \u003d sorrs.getString(\""TABLE_COMMENTS\"");
                                                                      String sorcolumn \u003d sorrs.getString(\""COLUMN_NAME\"");
                                                                      String sorcolumnco \u003d sorrs.getString(\""COLUMN_COMMENTS\"");
                                                                      String sortype \u003d sorrs.getString(\""DATA_TYPE\"");
                                                                      String sorlength \u003d sorrs.getString(\""DATA_LENGTH\"");
                                                                      String sorprecision \u003d sorrs.getString(\""DATA_PRECISION\"");
                                                                      String sorscale \u003d sorrs.getString(\""DATA_SCALE\"");
                                                                      String sornull \u003d sorrs.getString(\""NULLABLE\"");
                                                                      int sorid \u003d sorrs.getInt(\""COLUMN_ID\"");
                                                                      String sordefaultlen \u003d sorrs.getString(\""DEFAULT_LENGTH\"");
                                                                      String sordefault \u003d sorrs.getString(\""DATA_DEFAULT\"").trim();
                                                                      String sorpk \u003d sorrs.getString(\""PK_YN\"");
                                                                      String sorcollen \u003d sorrs.getString(\""MAX_COL_LEN\"");
                                                                      String sormaxcollen \u003d sorrs.getString(\""PK_MAX_COL_LEN\"");

String LD0040sql \u003d \""\"";
if (sorid \u003d\u003d 1) {
LD0040sql \u003d \""                (\""+sorcolumn;
} else {
LD0040sql \u003d \""                ,\""+sorcolumn;
}
Insert +\u003d LD0040sql + \""\\r\
\"";

String LDsql \u003d \""                \"";
if(sorid \u003d\u003d 1) {
LDsql +\u003d \"" \"";
} else {
LDsql +\u003d \"",\"";
}
if (sorcolumn.equals(\""DW_LDNG_DTTM\"")) {
LDsql +\u003d \""TO_CHAR(SYSTIMESTAMP, \u0027YYYYMMDDHH24MISSFF2\u0027)\"";
} else {
LDsql +\u003d sorcolumn;
}
Selectsql +\u003d LDsql + \""\\r\
\"";
}

String sqlLID0040Trc \u003d \""COMMIT;\\r\
\""
+ \""\\r\
\""
+ \""DECLARE\\r\
\""
+ \""      P_RTN              VARCHAR2(1000);\\r\
\""
+ \""BEGIN\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION SET NLS_DATE_FROMAT \u003d \\\\\\\""YYYYMMDD\\\\\\\""\u0027;\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION SET NLS_TIMESTAMP_FORMAT \u003d \\\\\\\""YYYYMMDDHH24MISS\\\\\\\""\u0027;\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION ENABLE PARALLEL DML\u0027;\\r\
\""
+ \""\\r\
\""
+ \""           INSERT /* \"" + sUser + \"" /ADW/INI/\"" + sub + \""/\"" + LID + \"" - US_0040 */\\r\
\""
+ \""                            /* APPEND PARALLEL(4) */ \\r\
\""
+ \""                 INTO SOROWN.\""+ tblID + \""\\r\
\""
+ Insertsql
+ \""                           )\\r\
\""
+ \""           SELECT /*+ FULL(T10) PARALLEL(4) */\\r\
\""
+ Selectsql
+ \""                FROM STGOWN.\""+ tblID + \"" T10\\r\
\""
+ \""              ;\\r\
\""
+ \""\\r\
\""
+ \""            O_COUNT :\u003d SQL%ROWCOUNT;\\r\
\""
+ \""            O_ERRMSG :\u003d SQLERRM;\\r\
\""
+ \""\\r\
\""
+ \""           COMMIT;\\r\
\""
+ \""END;\\r\
\""

String sqlLCD0040Trc \u003d \""COMMIT;\\r\
\""
+ \""\\r\
\""
+ \""DECLARE\\r\
\""
+ \""      P_RTN              VARCHAR2(1000);\\r\
\""
+ \""BEGIN\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION SET NLS_DATE_FROMAT \u003d \\\\\\\""YYYYMMDD\\\\\\\""\u0027;\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION SET NLS_TIMESTAMP_FORMAT \u003d \\\\\\\""YYYYMMDDHH24MISS\\\\\\\""\u0027;\\r\
\""
+ \""           EXECUTE IMMEDIATE \u0027ALTER SESSION ENABLE PARALLEL DML\u0027;\\r\
\""
+ \""\\r\
\""
+ \""           INSERT /* \"" + sUser + \"" /ADW/CHG/\"" + sub + \""/\"" + LCD + \"" - US_0040 */\\r\
\""
+ \""                            /* APPEND PARALLEL(4) */ \\r\
\""
+ \""                 INTO SOROWN.\""+ tblID + \""\\r\
\""
+ Insertsql
+ \""                           )\\r\
\""
+ \""           SELECT /*+ FULL(T10) PARALLEL(4) */\\r\
\""
+ Selectsql
+ \""                FROM STGOWN.\""+ tblID + \"" T10\\r\
\""
+ \""              ;\\r\
\""
+ \""\\r\
\""
+ \""            O_COUNT :\u003d SQL%ROWCOUNT;\\r\
\""
+ \""            O_ERRMSG :\u003d SQLERRM;\\r\
\""
+ \""\\r\
\""
+ \""           COMMIT;\\r\
\""
+ \""END;\\r\
\""


File LIDfile0040 \u003d new File(LIDfilePath0040);

if (!LIDfile0040.exists()) {

FileOutputStream fileOutStream \u003d new FileOutputStream(LIDfilePath0040);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlLID0040Trc);
bufferedWriter.close();
changeCount ++;
} else {
FileInputStream fileInStream \u003d new FileInputStream(LIDfilePath0040);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;
String LIDFile0040OldFile \u003d \""\"";

while((ch \u003d bufferedReader.read()) !\u003d -1) {

LIDFile0040OldFile +\u003d (char)ch;
}

if (LIDFile0040OldFile(sqlLID0040Trc)) {

} else if (!LIDFile0040OldFile(sqlLID0040Trc) \u0026\u0026 LIDfile0040.exists()) {
FileOutputStream fileOutStream \u003d new FileOutputStream(LIDfilePath0040);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlLID0040Trc);
bufferedWriter.close();
changeCount ++;
}
}

File LCDfile0040 \u003d new File(LCDfilePath0040);

if (!LCDfile0040.exists()) {

FileOutputStream fileOutStream \u003d new FileOutputStream(LCDfilePath0040);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlLCD0040Trc);
bufferedWriter.close();
changeCount ++;
} else {
FileInputStream fileInStream \u003d new FileInputStream(LCDfilePath0040);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;
String LCDFile0040OldFile \u003d \""\"";

while((ch \u003d bufferedReader.read()) !\u003d -1) {

LCDFile0040OldFile +\u003d (char)ch;
}

if (LCDFile0040OldFile(sqlLCD0040Trc)) {

} else if (!LCDFile0040OldFile(sqlLCD0040Trc) \u0026\u0026 LCDfile0040.exists()) {
FileOutputStream fileOutStream \u003d new FileOutputStream(LCDfilePath0040);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sqlLCD0040Trc);
bufferedWriter.close();
changeCount ++;
}
}
}

if (changeCount \u003d\u003d 0) {
System.out.println(\""[\"" + tblno + \""]\"" + \""[\"" + tblID + \""]\"" + \""[\"" + tblname + \""]\"" + \"" [Block 변경없음]\"");
notChangeCount++;
} else {
System.out.println(\""[\"" + tblno + \""]\"" + \""[\"" + tblID + \""]\"" + \""[\"" + tblname + \""]\"" + \"" [Block \"" + changeCount + \"" 개 생성완료 ]\"");
allChangeCount++;
}
pstmsor.close();
pstmcbs.close();
}
int allTablecnt \u003d notChangeCount + allChangeCount + ifTableCount + delTableCount;
System.out.println(\""\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\"");
System.out.println(\""총 \"" + delTableCount + \"" 건 테이블 삭제 ]\"");
System.out.println(\""총 \"" + ifTableCount + \"" 건 테이블 I/F적재형 ]\"");
System.out.println(\""총 \"" + notChangeCount + \"" 건 테이블 Block 변경 없음 ]\"");
System.out.println(\""총 \"" + allChangeCount + \"" 건 테이블 Block 생성 작업 완료 ]\"");
System.out.println(\""총 \"" + allTablecnt + \"" 건 테이블 작업 완료 ]\"");
System.out.println(\""\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\"");
} catch (SQLException | IOException sqle) {
System.out.println(\""예외발생\"");
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
return \""\"";
}
}
