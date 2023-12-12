public class GenTeraTs {

public String TeraTs() {

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
XSSFCell sUCDnm \u003d row.getCell(33);
XSSFCell sLCDnm \u003d row.getCell(36);
XSSFCell sUIDnm \u003d row.getCell(39);
XSSFCell sLIDnm \u003d row.getCell(42);

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
String UCDnm \u003d sUCDnm.getStringCellValue();
String LCDnm \u003d sLCDnm.getStringCellValue();
String UIDnm \u003d sUIDnm.getStringCellValue();
String LIDnm \u003d sLIDnm.getStringCellValue();
String DSNgb \u003d Dsngb.getStringCellValue();

String loadType \u003d \""CHG\"";
String loadType2 \u003d \""INI\"";

Date time \u003d new Date();

SimpleDateFormat JOB_DT_format \u003d new SimpleDateFormat(\""yyyyMMdd_HHmm\"");
SimpleDateFormat CR_DT_format \u003d new SimpleDateFormat(\""yyyy-MM-dd HH:mm:ss\"");

String JOB_DT \u003d JOB_DT_format.format(time);
String CR_DT \u003d CR_DT_format.format(time);

String GRP_INI \u003d \""\"";

if(sub.equals(\""DOP\"")) {
GRP_INI \u003d \""60\"";
} else if(sub.equals(\""DPD\"")) {
GRP_INI \u003d \""61\"";
} else if(sub.equals(\""DPL\"")) {
GRP_INI \u003d \""62\"";
} else if(sub.equals(\""DPV\"")) {
GRP_INI \u003d \""67\"";
} else if(sub.equals(\""DPG\"")) {
GRP_INI \u003d \""68\"";
} else if(sub.equals(\""DHC\"")) {
GRP_INI \u003d \""71\"";
} else if(sub.equals(\""DEC\"")) {
GRP_INI \u003d \""83\"";
} else if(sub.equals(\""DRU\"")) {
GRP_INI \u003d \""77\"";
} else if(sub.equals(\""DQH\"")) {
GRP_INI \u003d \""79\"";
} else if(sub.equals(\""DMM\"")) {
GRP_INI \u003d \""81\"";
} else if(sub.equals(\""DPF\"")) {
GRP_INI \u003d \""87\"";
} else if(sub.equals(\""DCA\"")) {
GRP_INI \u003d \""94\"";
}

String GRP_CHG \u003d \""\"";

if(sub.equals(\""DOP\"")) {
GRP_CHG \u003d \""63\"";
} else if(sub.equals(\""DPD\"")) {
GRP_CHG \u003d \""64\"";
} else if(sub.equals(\""DPL\"")) {
GRP_CHG \u003d \""65\"";
} else if(sub.equals(\""DPV\"")) {
GRP_CHG \u003d \""66\"";
} else if(sub.equals(\""DPG\"")) {
GRP_CHG \u003d \""69\"";
} else if(sub.equals(\""DHC\"")) {
GRP_CHG \u003d \""70\"";
} else if(sub.equals(\""DEC\"")) {
GRP_CHG \u003d \""82\"";
} else if(sub.equals(\""DRU\"")) {
GRP_CHG \u003d \""76\"";
} else if(sub.equals(\""DQH\"")) {
GRP_CHG \u003d \""78\"";
} else if(sub.equals(\""DMM\"")) {
GRP_CHG \u003d \""80\"";
} else if(sub.equals(\""DPF\"")) {
GRP_CHG \u003d \""86\"";
} else if(sub.equals(\""DCA\"")) {
GRP_CHG \u003d \""95\"";
}

                                                        if (gbChk.equals(\""삭제\"")) {
                                                                      delTableCount++;
                                                                      continue;
                                                        }

                                                        String loadchk \u003d tblloadchk.getStringCellValue().trim();

                                                        if (loadchk.equals(\""I/F적재형\"")) {
                                                                      ifTableCount++;
                                                                      continue;
                                                        }

if (tblsnps.equals(\""Y\"")) {

String sPgmPath \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\TS\\\\CHG\\\\TSP_\"" + LCD + \"".ts\"";
String sSrcPgmPath0010 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\\"" + sub + \""\\\\\"" + LCD + \""_US_0010.sql\"";
String sSrcPgmPath0020 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\\"" + sub + \""\\\\\"" + LCD + \""_US_0020.sql\"";
String sSrcPgmPath0030 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\\"" + sub + \""\\\\\"" + LCD + \""_US_0030.sql\"";

String sDescUS0010 \u003d \""스냅샷 파티션 Truncate\"";
String sDescUS0020 \u003d \""스냅샷 Insert\"";
String sDescUS0030 \u003d \""스냅샷 파티션 통계 생성\"";

String sUS_0010 \u003d \""\"";
String sUS_0020 \u003d \""\"";
String sUS_0030 \u003d \""\"";

File file0010 \u003d new File(sSrcPgmPath0010);
if (!file0010.exists()) {
System.out.println(\""[\"" + tblno + \""]\"" + \""[\"" + tblID + \""]\"" + \""[\"" + tblname + \""]\"" + \""[\"" + LCD + \""]\"" + \"" 파일이 없어!\"");
} else {
FileInputStream fileInStream \u003d new FileInputStream(file0010);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;

while((ch \u003d bufferedReader.read()) !\u003d -1) {
sUS_0010 +\u003d (char)ch;
}
}

File file0020 \u003d new File(sSrcPgmPath0020);
if (!file0020.exists()) {
System.out.println(\""[\"" + tblno + \""]\"" + \""[\"" + tblID + \""]\"" + \""[\"" + tblname + \""]\"" + \""[\"" + LCD + \""]\"" + \"" 파일이 없어!\"");
} else {
FileInputStream fileInStream \u003d new FileInputStream(file0020);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;

while((ch \u003d bufferedReader.read()) !\u003d -1) {
sUS_0020 +\u003d (char)ch;
}
}

File file0030 \u003d new File(sSrcPgmPath0030);
if (!file0030.exists()) {
System.out.println(\""[\"" + tblno + \""]\"" + \""[\"" + tblID + \""]\"" + \""[\"" + tblname + \""]\"" + \""[\"" + LCD + \""]\"" + \"" 파일이 없어!\"");
} else {
FileInputStream fileInStream \u003d new FileInputStream(file0030);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;

while((ch \u003d bufferedReader.read()) !\u003d -1) {
sUS_0030 +\u003d (char)ch;
}
}

String sTs \u003d \""file\u003dtsscm1.2\
\""
+ \""/prj_grp \"" + GRP_CHG + \"" adwsor /ADW/\"" + loadType + \""/\"" + sub + \""\
\""
+ \""$prj 0\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \""*prj_grp_id\u003d\\\""\"" + GRP_CHG + \""\\\""\
\""
+ \""!nm\u003d\\\""\"" + LCD + \""\\\""\
\""
+ \"".desc11\u003d\\\""\"" + LCDnm + \""\\\""\
\""
+ \"".cur_ver_id\u003d\\\""2\\\""\
\""
+ \"".owner_nm\u003d\\\""adwsor\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT _ \""\\\""\
\""
+ \"".up_dt\u003d\\\""\"" + CR_DT _ \""\\\""\
\""
+ \"".perm\u003d\\\""774\\\""\
\""
+ \"".grp_nm\u003d\\\""adwsor\\\""\
\""
+ \"".favorite_yn\u003d\\\""N\\\""\
\""
+ \"".status_cd\u003d\\\""0\\\""\
\""
+ \"".sms_id\u003d\\\""0\\\""\
\""
+ \"".email_id\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$arrow_dtl 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""2\\\""\
\""
+ \"".sequence\u003d\\\""1\\\""\
\""
+ \"".sequence2\u003d\\\""1\\\""\
\""
+ \"".pos_x\u003d\\\""82\\\""\
\""
+ \"".pos_y\u003d\\\""249\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""2\\\""\
\""
+ \"".sequence\u003d\\\""1\\\""\
\""
+ \"".sequence2\u003d\\\""2\\\""\
\""
+ \"".pos_x\u003d\\\""166\\\""\
\""
+ \"".pos_y\u003d\\\""249\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""3\\\""\
\""
+ \"".sequence\u003d\\\""2\\\""\
\""
+ \"".sequence2\u003d\\\""1\\\""\
\""
+ \"".pos_x\u003d\\\""202\\\""\
\""
+ \"".pos_y\u003d\\\""248\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""3\\\""\
\""
+ \"".sequence\u003d\\\""2\\\""\
\""
+ \"".sequence2\u003d\\\""2\\\""\
\""
+ \"".pos_x\u003d\\\""286\\\""\
\""
+ \"".pos_y\u003d\\\""248\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$block 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""2\\\""\
\""
+ \"".nm\u003d\\\""US_0010\\\""\
\""
+ \"".block_kind_cd\u003d\\\""11\\\""\
\""
+ \"".block_status_cd\u003d\\\""1\\\""\
\""
+ \"".parallel\u003d\\\""1\\\""\
\""
+ \"".comp\u003d\\\""N\\\""\
\""
+ \"".pos_x\u003d\\\""30\\\""\
\""
+ \"".pos_y\u003d\\\""230\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDescUs0010 + \""\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".up_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".block_kind_before\u003d\\\""0\\\""\
\""
+ \"".block_kind_arter\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""3\\\""\
\""
+ \"".nm\u003d\\\""US_0020\\\""\
\""
+ \"".block_kind_cd\u003d\\\""11\\\""\
\""
+ \"".block_status_cd\u003d\\\""1\\\""\
\""
+ \"".parallel\u003d\\\""1\\\""\
\""
+ \"".comp\u003d\\\""N\\\""\
\""
+ \"".pos_x\u003d\\\""150\\\""\
\""
+ \"".pos_y\u003d\\\""230\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDescUs0020 + \""\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".up_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".block_kind_before\u003d\\\""0\\\""\
\""
+ \"".block_kind_arter\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""4\\\""\
\""
+ \"".nm\u003d\\\""US_0030\\\""\
\""
+ \"".block_kind_cd\u003d\\\""11\\\""\
\""
+ \"".block_status_cd\u003d\\\""1\\\""\
\""
+ \"".parallel\u003d\\\""1\\\""\
\""
+ \"".comp\u003d\\\""N\\\""\
\""
+ \"".pos_x\u003d\\\""270\\\""\
\""
+ \"".pos_y\u003d\\\""230\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDescUs0030 + \""\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".up_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".block_kind_before\u003d\\\""0\\\""\
\""
+ \"".block_kind_arter\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$dep 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""2\\\""\
\""
+ \"".sequence\u003d\\\""1\\\""\
\""
+ \"".dep_type_cd\u003d\\\""1\\\""\
\""
+ \"".target_block_id\u003d\\\""3\\\""\
\""
+ \"".link_kind_cd\u003d\\\""1\\\""\
\""
+ \"".link_type_cd\u003d\\\""1\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""3\\\""\
\""
+ \"".sequence\u003d\\\""2\\\""\
\""
+ \"".dep_type_cd\u003d\\\""1\\\""\
\""
+ \"".target_block_id\u003d\\\""4\\\""\
\""
+ \"".link_kind_cd\u003d\\\""1\\\""\
\""
+ \"".link_type_cd\u003d\\\""1\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$procedure_block 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""2\\\""\
\""
+ \"".db_nm\u003d\\\""SOR\\\""\
\""
+ \"".nm\u003d\\\""US_0010\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDescUS0010 + \""\\\""\
\""
+ \"".sql_source\u003d\\\""set serveroutput on\
\""
+ \""Declare\
\""
+ \""      o_result int;\
\""
+ \""      o_errmsg varchar2(4000);\
\""
+ \""\
\""
+ \""BEGIN\
\""
+ sUS_0010 + \""\
\""
+ \""     dbms_output.put_line(\u0027o_result : \u0027|| o_result);\
\""
+ \""     dbms_output.put_line(\u0027o_errmsg : \u0027|| o_errmsg);\
\""
+ \""END;\
\""
+ \""\\\""\
\""
+ \"".sp_sql\u003d\\\""1\\\""\
\""
+ \"".sp1\u003d\\\""0\\\""\
\""
+ \"".sp2\u003d\\\""1\\\""\
\""
+ \"".sp3\u003d\\\""0\\\""\
\""
+ \"".sp4\u003d\\\""0\\\""\
\""
+ \"".sp_text\u003d\\\""\"" + sUS_0010 + \""\\\""\
\""
+ \"".sysbase_native_dsn\u003d\\\""\\\""\
\""
+ \"".sp_option\u003d\\\""\\\""\
\""
+ \"".ifiller1\u003d\\\""1\\\""\
\""
+ \"".ifiller2\u003d\\\""0\\\""\
\""
+ \"".ifiller3\u003d\\\""0\\\""\
\""
+ \"".sfiller1\u003d\\\""\\\""\
\""
+ \"".sfiller2\u003d\\\""\\\""\
\""
+ \"".sfiller3\u003dNULL\
\""
+ \"".stdout_file\u003dNULL\
\""
+ \"".stderr_file\u003dNULL\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""3\\\""\
\""
+ \"".db_nm\u003d\\\""SOR\\\""\
\""
+ \"".nm\u003d\\\""US_0020\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDescUS0020 + \""\\\""\
\""
+ \"".sql_source\u003d\\\""set serveroutput on\
\""
+ \""Declare\
\""
+ \""      o_result int;\
\""
+ \""      o_errmsg varchar2(4000);\
\""
+ \""      o_count int;
+ \""\
\""
+ \""BEGIN\
\""
+ sUS_0020 + \""\
\""
+ \""     dbms_output.put_line(\u0027o_result : \u0027|| o_result);\
\""
+ \""     dbms_output.put_line(\u0027o_errmsg : \u0027|| o_errmsg);\
\""
+ \""     dbms_output.put_line(\u0027o_count : \u0027|| o_count);\
\""
+ \""END;\
\""
+ \""\\\""\
\""
+ \"".sp_sql\u003d\\\""1\\\""\
\""
+ \"".sp1\u003d\\\""0\\\""\
\""
+ \"".sp2\u003d\\\""1\\\""\
\""
+ \"".sp3\u003d\\\""1\\\""\
\""
+ \"".sp4\u003d\\\""0\\\""\
\""
+ \"".sp_text\u003d\\\""\"" + sUS_0020 + \""\\\""\
\""
+ \"".sysbase_native_dsn\u003d\\\""\\\""\
\""
+ \"".sp_option\u003d\\\""\\\""\
\""
+ \"".ifiller1\u003d\\\""1\\\""\
\""
+ \"".ifiller2\u003d\\\""0\\\""\
\""
+ \"".ifiller3\u003d\\\""0\\\""\
\""
+ \"".sfiller1\u003d\\\""\\\""\
\""
+ \"".sfiller2\u003d\\\""\\\""\
\""
+ \"".sfiller3\u003dNULL\
\""
+ \"".stdout_file\u003dNULL\
\""
+ \"".stderr_file\u003dNULL\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""4\\\""\
\""
+ \"".db_nm\u003d\\\""SOR\\\""\
\""
+ \"".nm\u003d\\\""US_0030\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDescUS0030 + \""\\\""\
\""
+ \"".sql_source\u003d\\\""set serveroutput on\
\""
+ \""Declare\
\""
+ \""      o_result int;\
\""
+ \""      o_errmsg varchar2(4000);\
\""
+ \""      o_count int;
+ \""\
\""
+ \""BEGIN\
\""
+ sUS_0030 + \""\
\""
+ \""     dbms_output.put_line(\u0027o_result : \u0027|| o_result);\
\""
+ \""     dbms_output.put_line(\u0027o_errmsg : \u0027|| o_errmsg);\
\""
+ \""     dbms_output.put_line(\u0027o_count : \u0027|| o_count);\
\""
+ \""END;\
\""
+ \""\\\""\
\""
+ \"".sp_sql\u003d\\\""1\\\""\
\""
+ \"".sp1\u003d\\\""0\\\""\
\""
+ \"".sp2\u003d\\\""1\\\""\
\""
+ \"".sp3\u003d\\\""1\\\""\
\""
+ \"".sp4\u003d\\\""0\\\""\
\""
+ \"".sp_text\u003d\\\""\"" + sUS_0030 + \""\\\""\
\""
+ \"".sysbase_native_dsn\u003d\\\""\\\""\
\""
+ \"".sp_option\u003d\\\""\\\""\
\""
+ \"".ifiller1\u003d\\\""1\\\""\
\""
+ \"".ifiller2\u003d\\\""0\\\""\
\""
+ \"".ifiller3\u003d\\\""0\\\""\
\""
+ \"".sfiller1\u003d\\\""\\\""\
\""
+ \"".sfiller2\u003d\\\""\\\""\
\""
+ \"".sfiller3\u003dNULL\
\""
+ \"".stdout_file\u003dNULL\
\""
+ \"".stderr_file\u003dNULL\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$prj_ver 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".ver_nm\u003d\\\""\\\""\
\""
+ \"".desc1\u003d\\\""\u003cviewsize\u003e1000;1000\u003c/viewsize\u003e\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".up_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$note_memo 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""1\\\""\
\""
+ \"".nm\u003d\\\""프로젝트 정보\\\""\
\""
+ \"".block_kind_cd\u003d\\\""13\\\""\
\""
+ \"".block_status_cd\u003d\\\""1\\\""\
\""
+ \"".pos_x\u003d\\\""787404\\\""\
\""
+ \"".pos_y\u003d\\\""786610\\\""\
\""
+ \"".desc1\u003d\\\""bsEllipse\
\""
+ \""1. 프로젝트 명 : \"" + LCD + \"" - \"" + LCDnm + \""\
\""
+ \""2. 변경이력\
\""
+ \""     일자                      작성자                 변경내용\
\""
+ \""     ---------------  -------------   -----------------------------------------------------------------------------------------------
+ \""     2020-09-09    ADW                   최초작성\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".up_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".block_kind_before\u003d\\\""0\\\""\
\""
+ \"".block_kind_after\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""\"";


File TSfile \u003d new File(sPgmPath);

if (!TSfile.exists()) {

FileOutputStream fileOutStream \u003d new FileOutputStream(sPgmPath);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sTs);
bufferedWriter.close();
chgCount ++;
} else {
FileInputStream fileInStream \u003d new FileInputStream(sPgmPath);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;
String TSfileoldFile \u003d \""\"";

while((ch \u003d bufferedReader.read()) !\u003d -1) {

TSfileoldFile +\u003d (char)ch;
}

String tsFileOldFileBe \u003d TSfileoldFile.replaceAll(\""202[0-9].{15}\"", \""\"");
String sTsBe \u003d sTs.replaceAll(\""202[0-9].{15}\"", \""\"");

if (tsFileOldFileBe.equals(sTsBe)) {

} else if (!tsFileOldFileBe.equals(sTsBe) \u0026\u0026 TSfile.exists()) {
FileOutputStream fileOutStream \u003d new FileOutputStream(sPgmPath);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sTs);
bufferedWriter.close();
chgCount ++;
}
}
}

if (!UCD.equals(null) \u0026\u0026 !UID.equals(null) \u0026\u0026 !tblsnps.equals(\""Y\"")) {

String sPgmchgPath \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\TS\\\\CHG\\\\TSP_\"" + UCD + \"".ts\"";
String sPgminiPath \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\TS\\\\INI\\\\TSP_\"" + UID + \"".ts\"";
String sSrcPgmchgPath0010 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\\"" + sub + \""\\\\\"" + UCD + \""_UN_0010.sql\"";
String sSrcPgminiPath0010 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\INI\\\\\"" + sub + \""\\\\\"" + UID + \""_UN_0010.sql\"";

String sDsn \u003d \""BCV\"";

if (!DSNgb.equals(null) \u0026\u0026 !DSNgb.equals(\""\"")) {
sDSN \u003d DSNgb;
}

String sDescINIUS0010 \u003d \""초기적재 UNLOAD\"";
String sDescCHGUS0010 \u003d \""변경적재 UNLOAD\"";

String sChgUS_0010 \u003d \""\"";
String sIniUS_0020 \u003d \""\"";

File fileCHG \u003d new File(sSrcPgmchgPath0010);
if (!fileCHG.exists()) {
System.out.println(\""[\"" + tblno + \""]\"" + \""[\"" + tblID + \""]\"" + \""[\"" + tblname + \""]\"" + \""[\"" + UCD + \""]\"" + \"" 파일이 없어!\"");
} else {
FileInputStream fileInStream \u003d new FileInputStream(sSrcPgmchgPath0010);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;

while((ch \u003d bufferedReader.read()) !\u003d -1) {
sChgUS_0010 +\u003d (char)ch;
}
}

File fileINI \u003d new File(sSrcPgminiPath0010);
if (!fileINI.exists()) {
System.out.println(\""[\"" + tblno + \""]\"" + \""[\"" + tblID + \""]\"" + \""[\"" + tblname + \""]\"" + \""[\"" + UID + \""]\"" + \"" 파일이 없어!\"");
} else {
FileInputStream fileInStream \u003d new FileInputStream(sSrcPgminiPath0010);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;

while((ch \u003d bufferedReader.read()) !\u003d -1) {
sIniUS_0010 +\u003d (char)ch;
}
}

String INI \u003d \""file\u003dtsscm1.2\
\""
+ \""/prj_grp \"" + GRP_INI + \"" adwsor /ADW/\"" + loadType2 + \""/\"" + sub + \""\
\""
+ \""$prj 0\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \""*prj_grp_id\u003d\\\""\"" + GRP_INI + \""\\\""\
\""
+ \""!nm\u003d\\\""\"" + UID + \""\\\""\
\""
+ \"".desc11\u003d\\\""\"" + UIDnm + \""\\\""\
\""
+ \"".cur_ver_id\u003d\\\""1\\\""\
\""
+ \"".owner_nm\u003d\\\""adwsor\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT _ \""\\\""\
\""
+ \"".up_dt\u003d\\\""\"" + CR_DT _ \""\\\""\
\""
+ \"".perm\u003d\\\""774\\\""\
\""
+ \"".grp_nm\u003d\\\""adwsor\\\""\
\""
+ \"".favorite_yn\u003d\\\""N\\\""\
\""
+ \"".status_cd\u003d\\\""0\\\""\
\""
+ \"".sms_id\u003d\\\""0\\\""\
\""
+ \"".email_id\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$block 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""2\\\""\
\""
+ \"".nm\u003d\\\""UN_0010\\\""\
\""
+ \"".block_kind_cd\u003d\\\""8\\\""\
\""
+ \"".block_status_cd\u003d\\\""5\\\""\
\""
+ \"".parallel\u003d\\\""1\\\""\
\""
+ \"".comp\u003d\\\""N\\\""\
\""
+ \"".pos_x\u003d\\\""30\\\""\
\""
+ \"".pos_y\u003d\\\""230\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDescINIUS0010 + \""\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".up_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".block_kind_before\u003d\\\""0\\\""\
\""
+ \"".block_kind_after\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$unload2 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""2\\\""\
\""
+ \"".db_nm\u003d\\\""\"" + sDSN + \""\\\""\
\""
+ \"".ffd_id\u003d\\\""20\\\""\
\""
+ \"".db_type_cd\u003d\\\""2\\\""\
\""
+ \"".sybase_native_dsn\u003d\\\""\
\""
+ \"".user_db_nm\u003d\\\""\
\""
+ \"".user_nm\u003d\\\""\
\""
+ \"".passwd\u003d\\\""\
\""
+ \"".query\u003d\\\""\"" + sIniUS_0010 + \""\\\""\
\""
+ \"".outfile1\u003d\\\""$$[SOR_\"" + loadType2 + \""_SAM]/\"" + sub + \""/\"" + tblID + \"".dat\\\""\
\""
+ \"".type-\\\""2\\\""\
\""
+ \"".delim\u003d\\\""|^,\\\""\
\""
+ \"".field1\u003d\\\""\
\""
+ \"".field2\u003d\\\""1024\
\""
+ \"".field3\u003d\\\""\
\""
+ \"".field4\u003d\\\""YYYYMMDD\
\""
+ \"".field5\u003d\\\""\
\""
+ \"".field6\u003d\\\""\
\""
+ \"".field7\u003d\\\""\
\""
+ \"".field8\u003d\\\""Y\
\""
+ \"".field9\u003d\\\""\\\""\
\""
+ \"".connect_type\u003d\\\""1\\\""\
\""
+ \"".sybase_quote_yn\u003d\\\""N\\\""\
\""
+ \"".ffd_use_yn\u003d\\\""N\\\""\
\""
+ \"".table_ffd_id\u003d\\\""0\\\""\
\""
+ \"".line_delim_yn\u003d\\\""N\\\""\
\""
+ \"".ifiller1\u003d\\\""0\\\""\
\""
+ \"".ifiller2\u003d\\\""0\\\""\
\""
+ \"".ifiller3\u003d\\\""0\\\""\
\""
+ \"".sfiller1\u003d\\\""\\\""\
\""
+ \"".sfiller2\u003d\\\""\\\""\
\""
+ \"".sfiller3\u003d\\\""\\\""\
\""
+ \"".ifiller4\u003d\\\""0\\\""\
\""
+ \"".ifiller5\u003d\\\""0\\\""\
\""
+ \"".ifiller6\u003d\\\""1\\\""\
\""
+ \"".sfiller4\u003d\\\""\\\""\
\""
+ \"".sfiller5\u003d\\\""\\\""\
\""
+ \"".sfiller6\u003d\\\""\\\""\
\""
+ \"".options\u003d\\\""\
\""
+ \"".data_type_cd\u003d\\\""1\\\""\
\""
+ \"".dfs_nm\u003d\\\""LOCAL\\\""\
\""
+ \"".timestamp_format\u003d\\\""YYYYMMDDHH24MISSFF6\\\""\
\""
+ \"".time_format\u003d\\\""\
\""
+ \"".enf_row_delim\u003dNULL\
\""
+ \"".use_rsc\u003d\\\""N\\\""\
\""
+ \"".rsc_options\u003d\\\""\
\""
+ \"".sqoop_use\u003d\\\""0\\\""\
\""
+ \"".cond_clause\u003d\\\""\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$note_memo 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""1\\\""\
\""
+ \"".nm\u003d\\\""프로젝트 정보\\\""\
\""
+ \"".block_kind_cd\u003d\\\""13\\\""\
\""
+ \"".block_status_cd\u003d\\\""1\\\""\
\""
+ \"".pos_x\u003d\\\""787404\\\""\
\""
+ \"".pos_y\u003d\\\""786610\\\""\
\""
+ \"".desc1\u003d\\\""bsEllipse\
\""
+ \""1. 프로젝트 명 : \"" + UID + \"" - \"" + UIDnm + \""\
\""
+ \""2. 변경이력\
\""
+ \""     일자                      작성자                 변경내용\
\""
+ \""     ---------------  -------------   -----------------------------------------------------------------------------------------------
+ \""     2020-09-09    ADW                   최초작성\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".up_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".block_kind_before\u003d\\\""0\\\""\
\""
+ \"".block_kind_after\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""\"";

String CHG \u003d \""file\u003dtsscm1.2\
\""
+ \""/prj_grp \"" + GRP_CHG + \"" adwsor /ADW/\"" + loadType + \""/\"" + sub + \""\
\""
+ \""$prj 0\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \""*prj_grp_id\u003d\\\""\"" + GRP_CHG + \""\\\""\
\""
+ \""!nm\u003d\\\""\"" + UCD + \""\\\""\
\""
+ \"".desc11\u003d\\\""\"" + UCDnm + \""\\\""\
\""
+ \"".cur_ver_id\u003d\\\""1\\\""\
\""
+ \"".owner_nm\u003d\\\""adwsor\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT _ \""\\\""\
\""
+ \"".up_dt\u003d\\\""\"" + CR_DT _ \""\\\""\
\""
+ \"".perm\u003d\\\""774\\\""\
\""
+ \"".grp_nm\u003d\\\""adwsor\\\""\
\""
+ \"".favorite_yn\u003d\\\""N\\\""\
\""
+ \"".status_cd\u003d\\\""0\\\""\
\""
+ \"".sms_id\u003d\\\""0\\\""\
\""
+ \"".email_id\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$block 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""2\\\""\
\""
+ \"".nm\u003d\\\""UN_0010\\\""\
\""
+ \"".block_kind_cd\u003d\\\""8\\\""\
\""
+ \"".block_status_cd\u003d\\\""5\\\""\
\""
+ \"".parallel\u003d\\\""1\\\""\
\""
+ \"".comp\u003d\\\""N\\\""\
\""
+ \"".pos_x\u003d\\\""30\\\""\
\""
+ \"".pos_y\u003d\\\""230\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDescCHGUS0010 + \""\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".up_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".block_kind_before\u003d\\\""0\\\""\
\""
+ \"".block_kind_after\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$unload2 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""2\\\""\
\""
+ \"".db_nm\u003d\\\""\"" + sDSN + \""\\\""\
\""
+ \"".ffd_id\u003d\\\""20\\\""\
\""
+ \"".db_type_cd\u003d\\\""2\\\""\
\""
+ \"".sybase_native_dsn\u003d\\\""\
\""
+ \"".user_db_nm\u003d\\\""\
\""
+ \"".user_nm\u003d\\\""\
\""
+ \"".passwd\u003d\\\""\
\""
+ \"".query\u003d\\\""\"" + sChgUS_0010 + \""\\\""\
\""
+ \"".outfile1\u003d\\\""$$[SOR_\"" + loadType + \""_SAM]/\"" + sub + \""/\"" + tblID + \"".dat\\\""\
\""
+ \"".type-\\\""2\\\""\
\""
+ \"".delim\u003d\\\""|^,\\\""\
\""
+ \"".field1\u003d\\\""\
\""
+ \"".field2\u003d\\\""1024\
\""
+ \"".field3\u003d\\\""\
\""
+ \"".field4\u003d\\\""YYYYMMDD\
\""
+ \"".field5\u003d\\\""\
\""
+ \"".field6\u003d\\\""\
\""
+ \"".field7\u003d\\\""\
\""
+ \"".field8\u003d\\\""Y\
\""
+ \"".field9\u003d\\\""\\\""\
\""
+ \"".connect_type\u003d\\\""1\\\""\
\""
+ \"".sybase_quote_yn\u003d\\\""N\\\""\
\""
+ \"".ffd_use_yn\u003d\\\""N\\\""\
\""
+ \"".table_ffd_id\u003d\\\""0\\\""\
\""
+ \"".line_delim_yn\u003d\\\""N\\\""\
\""
+ \"".ifiller1\u003d\\\""0\\\""\
\""
+ \"".ifiller2\u003d\\\""0\\\""\
\""
+ \"".ifiller3\u003d\\\""0\\\""\
\""
+ \"".sfiller1\u003d\\\""\\\""\
\""
+ \"".sfiller2\u003d\\\""\\\""\
\""
+ \"".sfiller3\u003d\\\""\\\""\
\""
+ \"".ifiller4\u003d\\\""0\\\""\
\""
+ \"".ifiller5\u003d\\\""0\\\""\
\""
+ \"".ifiller6\u003d\\\""1\\\""\
\""
+ \"".sfiller4\u003d\\\""\\\""\
\""
+ \"".sfiller5"
"\u003d\\\""\\\""\
\""
+ \"".sfiller6\u003d\\\""\\\""\
\""
+ \"".options\u003d\\\""\
\""
+ \"".data_type_cd\u003d\\\""1\\\""\
\""
+ \"".dfs_nm\u003d\\\""LOCAL\\\""\
\""
+ \"".timestamp_format\u003d\\\""YYYYMMDDHH24MISSFF6\\\""\
\""
+ \"".time_format\u003d\\\""\
\""
+ \"".enf_row_delim\u003dNULL\
\""
+ \"".use_rsc\u003d\\\""N\\\""\
\""
+ \"".rsc_options\u003d\\\""\
\""
+ \"".sqoop_use\u003d\\\""0\\\""\
\""
+ \"".cond_clause\u003d\\\""\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$note_memo 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""1\\\""\
\""
+ \"".nm\u003d\\\""프로젝트 정보\\\""\
\""
+ \"".block_kind_cd\u003d\\\""13\\\""\
\""
+ \"".block_status_cd\u003d\\\""1\\\""\
\""
+ \"".pos_x\u003d\\\""787404\\\""\
\""
+ \"".pos_y\u003d\\\""786610\\\""\
\""
+ \"".desc1\u003d\\\""bsEllipse\
\""
+ \""1. 프로젝트 명 : \"" + UCD + \"" - \"" + UCDnm + \""\
\""
+ \""2. 변경이력\
\""
+ \""     일자                      작성자                 변경내용\
\""
+ \""     ---------------  -------------   -----------------------------------------------------------------------------------------------
+ \""     2020-09-09    ADW                   최초작성\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".up_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".block_kind_before\u003d\\\""0\\\""\
\""
+ \"".block_kind_after\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""\"";

File TSUIDfile \u003d new File(sPgminiPath);

if (!TSUIDfile.exists()) {

FileOutputStream fileOutStream \u003d new FileOutputStream(sPgminiPath);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(INI);
bufferedWriter.close();
chgCount ++;
} else {
FileInputStream fileInStream \u003d new FileInputStream(sPgminiPath);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;
String TSUIDfileoldFile \u003d \""\"";

while((ch \u003d bufferedReader.read()) !\u003d -1) {

TSUIDfileoldFile +\u003d (char)ch;
}

String tsFileOldFileBe \u003d TSUIDfileoldFile.replaceAll(\""202[0-9].{15}\"", \""\"");
String sTsBe \u003d INI.replaceAll(\""202[0-9].{15}\"", \""\"");

if (tsFileOldFileBe.equals(sTsBe)) {

} else if (!tsFileOldFileBe.equals(sTsBe)) {
FileOutputStream fileOutStream \u003d new FileOutputStream(sPgminiPath);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(INI);
bufferedWriter.close();
chgCount ++;
}
}

File TSUCDfile \u003d new File(sPgmchgPath);

if (!TSUCDfile.exists()) {

FileOutputStream fileOutStream \u003d new FileOutputStream(sPgmchgPath);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(CHG);
bufferedWriter.close();
chgCount ++;
} else {
FileInputStream fileInStream \u003d new FileInputStream(sPgmchgPath);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;
String TSUCDfileoldFile \u003d \""\"";

while((ch \u003d bufferedReader.read()) !\u003d -1) {

TSUCDfileoldFile +\u003d (char)ch;
}

String tsFileOldFileBe \u003d TSUCDfileoldFile.replaceAll(\""202[0-9].{15}\"", \""\"");
String sTsBe \u003d CHG.replaceAll(\""202[0-9].{15}\"", \""\"");

if (tsFileOldFileBe.equals(sTsBe)) {

} else if (!tsFileOldFileBe.equals(sTsBe) \u0026\u0026 TSUCDfile.exists()) {
FileOutputStream fileOutStream \u003d new FileOutputStream(sPgmchgPath);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(CHG);
bufferedWriter.close();
chgCount ++;
}
}
}

if(!LCD.equals(null) \u0026\u0026 !LID.equals(null) \u0026\u0026 !tblsnps.equals(\""Y\"")) {
pstmcbs \u003d conncbs.prepareStatement(cbsQuary.ResultSet.TYPE_SCROLL_INSENSITIVE.ResultSet.CONCUR_UPDATABLE);
pstmcbs.setString(1, \""C\"" + tblID.substring(1));
cbsrs \u003d pstmcbs.executeQuery();

cbsrs.last();

int cbsrowcount \u003d 0;
cbsrowcount \u003d cbsrs.getRow();

cbsrs.beforeFirst();

String arTblInfo \u003d \""\"";
String arFFDInfo \u003d \""\"";
int nFileLoc \u003d 1;

                                                        while (cbsRs.next()) {
                                                                      String cbsOwner \u003d cbsRs.getString(\""OWNER\"");
                                                                      String cbsTblNm \u003d cbsRs.getString(\""TABLE_NAME\"");
                                                                      String cbsTblCo \u003d cbsRs.getString(\""TABLE_COMMENTS\"");
                                                                      String cbsColumn \u003d cbsRs.getString(\""COLUMN_NAME\"");
                                                                      String cbsColumnCo \u003d cbsRs.getString(\""COLUMN_COMMENTS\"");
                                                                      String cbsType \u003d cbsRs.getString(\""DATA_TYPE\"");
                                                                      String cbsLength \u003d cbsRs.getString(\""DATA_LENGTH\"");
                                                                      String cbsPrecision \u003d cbsRs.getString(\""DATA_PRECISION\"");
                                                                      String cbsScale \u003d cbsRs.getString(\""DATA_SCALE\"");
                                                                      String cbsNull \u003d cbsRs.getString(\""NULLABLE\"");
                                                                      int cbsId \u003d cbsRs.getInt(\""COLUMN_ID\"");
                                                                      String cbsDefaultLen \u003d cbsRs.getString(\""DEFAULT_LENGTH\"");
                                                                      String cbsDataDefault \u003d cbsRs.getString(\""DATA_DEFAULT\"").trim();
                                                                      String cbsPk \u003d cbsRs.getString(\""PK_YN\"");

String tblDataType \u003d \""\"";
String ffdDataType \u003d \""\"";
inf ffdDataPrec \u003d 0;
int ffdDataScal \u003d 0;
int ffdDataSize \u003d 0;
String ffdformat \u003d \""\"";
String ffdNull \u003d \""0\"";

String arTblInfocol \u003d \""\"";
String arFFDInfocol \u003d \""\"";

if (cbstype.contains(\""TIMESTAMP\"")) {
cbstype \u003d \""TIMESTAMP\"";
tblDataType \u003d \""11\"";
ffdDataType \u003d \""3\"";
ffdDataPrec \u003d 11;
ffdDataScal \u003d 0;
ffdDataSize \u003d 14;
ffdformat \u003d \""YYYYMMDDHH24MISSFF6\"";
} else if (cbstype.contains(\""VARCHAR\"")) {
tblDataType \u003d \""12\"";
ffdDataType \u003d \""3\"";
ffdDataPrec \u003d cbslength;
ffdDataScal \u003d 0;
ffdDataSize \u003d cbslength;
} else if (cbstype.contains(\""NUMBER\"") \u0026\u0026 (cbsscale.equals(null) || cbsscale.equals(\""0\"") || cbsscale.equals(0))) {
tblDataType \u003d \""3\"";
ffdDataType \u003d \""2\"";
ffdDataPrec \u003d cbsprecision;
ffdDataScal \u003d 0;
ffdDataSize \u003d cbsprecision + 1;
} else if (cbstype.contains(\""NUMBER\"")) {
tblDataType \u003d \""6\"";
ffdDataType \u003d \""2\"";
ffdDataPrec \u003d cbsprecision;
ffdDataScal \u003d Integer.parseInt(cbsscale);
ffdDataSize \u003d cbsprecision + 2;
}

if (cbsnull.equals(\""N\"")) {
ffdNull \u003d \""1\"";
}

arTblInfocol \u003d \""+db_nm\u003ddb_nm\
\""
+ \""+table_nm\u003dnm\
\""
+ \"".#nm\u003d\\\""\"" + cbscolumn + \""\\\""\
\""
+ \"".field_data_type_cd\u003d\\\""\"" + tblDataType + \""\\\""\
\""
+ \"".field_scale\u003d\\\""\"" + ffdDataScal + \""\\\""\
\""
+ \"".field_prec\u003d\\\""\"" + ffdDataPrec + \""\\\""\
\""
+ \"".key_yn\u003d\\\""N\\\""\
\""
+ \"".desc1\u003d\\\""\\\""\
\""
+ \"".ifiller1\u003d\\\""0\\\""\
\""
+ \"".ifiller2\u003d\\\""0\\\""\
\""
+ \"".ifiller3\u003d\\\""0\\\""\
\""
+ \"".sfiller1\u003d\\\""STGOWN\\\""\
\""
+ \"".sfiller2\u003dNULL\
\""
+ \"".sfiller3\u003dNULL\
\""
+ \""$+\
\"";

arTblInfo +\u003d arTblInfocol;

afFFDInfocol \u003d \""+ffd_id\u003dffd_id\
\""
+ \""#ffd_col_id\u003d\\\""\"" + cbsid + \""\\\""\
\""
+ \"".nm\u003d\\\""\"" + cbscolumn + \""\\\""\
\""
+ \"".col_data_type_cd\u003d\\\""\"" + ffdDataType + \""\\\""\
\""
+ \"".col_sep\u003d\\\""|^,\\\""\
\""
+ \"".col_loc\u003d\\\""\"" + cbsid + \""\\\""\
\""
+ \"".col_size\u003d\\\""\"" + ffdDataSize + \""\\\""\
\""
+ \"".desc1\u003d\\\""\"" + cbscolumnco + \""\\\""\
\""
+ \"".prec\u003d\\\""\"" + ffdDataScal + \""\\\""\
\""
+ \"".key_yn\u003d\\\""N\\\""\
\""
+ \"".filler1\u003dNULL\
\""
+ \"".filler2\u003dNULL\
\""
+ \"".filler3\u003dNULL\
\""
+ \"".sql_type_cd\u003d\\\""\"" + tblDataType + \""\\\""\
\""
+ \"".col_pos\u003d\\\""\"" + nFileLoc + \""\\\""\
\""
+ \"".table_field_type\u003d\\\""\"" + cbstype + \""\\\""\
\""
+ \"".table_field_name\u003d\\\""\"" + cbscolumn + \""\\\""\
\""
+ \"".table_precision\u003d\\\""\"" + ffdDataPrec +\""\\\""\
\""
+ \"".table_scale\u003d\\\""\"" + ffdDataScal + \""\\\""\
\""
+ \"".table_PK_nullability\u003d\\\""\"" + ffdNull + \""\\\""\
\""
+ \"".table_default_value\u003d\\\""\\\""\
\""\""
+ \"".null_default_value\u003d\\\""\\\""\
\""
+ \"".table_date_format\u003d\\\""\"" + ffdformat + \""\\\""\
\""
+ \""$+\
\""


arFFDInfo +\u003d arFFDInfocol;
nFileLoc +\u003d ffdDataSize;

}

String schgPgmPath \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\TS\\\\CHG\\\\TSP_\"" + LCD + \"".ts\"";
String siniPgmPath \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\TS\\\\CHG\\\\TSP_\"" + LID + \"".ts\"";
String sSrciniPgmPath0010 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\INI\\\\\"" + sub + \""\\\\\"" + LID + \""_US_0010.sql\"";
String sSrciniPgmPath0020 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\INI\\\\\"" + sub + \""\\\\\"" + LID + \""_LD_0020.sql\"";
String sSrciniPgmPath0030 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\INI\\\\\"" + sub + \""\\\\\"" + LID + \""_US_0030.sql\"";
String sSrciniPgmPath0040 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\INI\\\\\"" + sub + \""\\\\\"" + LID + \""_US_0040.sql\"";
String sSrcchgPgmPath0010 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\\"" + sub + \""\\\\\"" + LCD + \""_US_0010.sql\"";
String sSrcchgPgmPath0010 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\\"" + sub + \""\\\\\"" + LCD + \""_LD_0020.sql\"";
String sSrcchgPgmPath0010 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\\"" + sub + \""\\\\\"" + LCD + \""_US_0030.sql\"";
String sSrcchgPgmPath0010 \u003d \""D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\\"" + sub + \""\\\\\"" + LCD + \""_US_0040.sql\"";

String sDesciniUS0010 \u003d \""Staging Truncate\"";
String sDesciniLD0020 \u003d \""Staging Load\"";
String sDesciniUS0030 \u003d \""SOR Truncate\"";
String sDesciniUS0040 \u003d \""SOR Insert\"";

String sDescchgUS0010 \u003d \""Staging Truncate\"";
String sDescchgLD0020 \u003d \""Staging Load\"";
String sDescchgUS0030 \u003d \""\"";
String sDescchgUS0040 \u003d \""SOR Insert\"";

if (loadchk.equals(\""초기적재형\"")) {
sDescchgUS0030 \u003d \""SOR Truncate\"";
} else {
sDescchgUS0030 \u003d \""SOR Delete\"";
}

String siniUS_0010 \u003d \""\"";
String siniLD_0020 \u003d \""\"";
String siniUS_0030 \u003d \""\"";
String siniUS_0040 \u003d \""\"";

String schgUS_0010 \u003d \""\"";
String schgLD_0020 \u003d \""\"";
String schgUS_0030 \u003d \""\"";
String schgUS_0040 \u003d \""\"";

File fileini0010 \u003d new File(sSrciniPgmPath0010);
if (!fileini0010.exists()) {
System.out.println(\""[\"" + tblno + \""]\"" + \""[\"" + tblID + \""]\"" + \""[\"" + tblname + \""]\"" + \""[\"" + LID + \""]\"" + \"" 파일이 없어!\"");
} else {
FileInputStream fileInStream \u003d new FileInputStream(sSrciniPgmPath0010);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;

while((ch \u003d bufferedReader.read()) !\u003d -1) {
siniUS_0010 +\u003d (char)ch;
}
}

File fileini0020 \u003d new File(sSrciniPgmPath0020);
if (!fileini0020.exists()) {
System.out.println(\""[\"" + tblno + \""]\"" + \""[\"" + tblID + \""]\"" + \""[\"" + tblname + \""]\"" + \""[\"" + LID + \""]\"" + \"" 파일이 없어!\"");
} else {
FileInputStream fileInStream \u003d new FileInputStream(sSrciniPgmPath0020);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;

while((ch \u003d bufferedReader.read()) !\u003d -1) {
siniLD_0020 +\u003d (char)ch;
}
}

File fileini0030 \u003d new File(sSrciniPgmPath0030);
if (!fileini0030.exists()) {
System.out.println(\""[\"" + tblno + \""]\"" + \""[\"" + tblID + \""]\"" + \""[\"" + tblname + \""]\"" + \""[\"" + LID + \""]\"" + \"" 파일이 없어!\"");
} else {
FileInputStream fileInStream \u003d new FileInputStream(sSrciniPgmPath0030);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;

while((ch \u003d bufferedReader.read()) !\u003d -1) {
siniUS_0030 +\u003d (char)ch;
}
}

File fileini0040 \u003d new File(sSrciniPgmPath0040);
if (!fileini0040.exists()) {
System.out.println(\""[\"" + tblno + \""]\"" + \""[\"" + tblID + \""]\"" + \""[\"" + tblname + \""]\"" + \""[\"" + LID + \""]\"" + \"" 파일이 없어!\"");
} else {
FileInputStream fileInStream \u003d new FileInputStream(sSrciniPgmPath0040);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;

while((ch \u003d bufferedReader.read()) !\u003d -1) {
siniUS_0040 +\u003d (char)ch;
}
}

File filechg0010 \u003d new File(sSrcchgPgmPath0010);
if (!filechg0010.exists()) {
System.out.println(\""[\"" + tblno + \""]\"" + \""[\"" + tblID + \""]\"" + \""[\"" + tblname + \""]\"" + \""[\"" + LCD + \""]\"" + \"" 파일이 없어!\"");
} else {
FileInputStream fileInStream \u003d new FileInputStream(sSrcchgPgmPath0010);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;

while((ch \u003d bufferedReader.read()) !\u003d -1) {
schgUS_0010 +\u003d (char)ch;
}
}

File filechg0020 \u003d new File(sSrcchgPgmPath0020);
if (!filechg0020.exists()) {
System.out.println(\""[\"" + tblno + \""]\"" + \""[\"" + tblID + \""]\"" + \""[\"" + tblname + \""]\"" + \""[\"" + LCD + \""]\"" + \"" 파일이 없어!\"");
} else {
FileInputStream fileInStream \u003d new FileInputStream(sSrcchgPgmPath0020);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;

while((ch \u003d bufferedReader.read()) !\u003d -1) {
schgLD_0020 +\u003d (char)ch;
}
}

File filechg0030 \u003d new File(sSrcchgPgmPath0030);
if (!filechg0030.exists()) {
System.out.println(\""[\"" + tblno + \""]\"" + \""[\"" + tblID + \""]\"" + \""[\"" + tblname + \""]\"" + \""[\"" + LCD + \""]\"" + \"" 파일이 없어!\"");
} else {
FileInputStream fileInStream \u003d new FileInputStream(sSrcchgPgmPath0030);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;

while((ch \u003d bufferedReader.read()) !\u003d -1) {
schgUS_0030 +\u003d (char)ch;
}
}

File filechg0040 \u003d new File(sSrcchgPgmPath0040);
if (!filechg0040.exists()) {
System.out.println(\""[\"" + tblno + \""]\"" + \""[\"" + tblID + \""]\"" + \""[\"" + tblname + \""]\"" + \""[\"" + LCD + \""]\"" + \"" 파일이 없어!\"");
} else {
FileInputStream fileInStream \u003d new FileInputStream(sSrcchgPgmPath0040);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;

while((ch \u003d bufferedReader.read()) !\u003d -1) {
schgUS_0040 +\u003d (char)ch;
}
}

String sTsINI \u003d\""file\u003dtsscm1.2\
\""
+ \""/ffd_grp 1 adwsor /DBFFD/ADW\
\""
+ \""/prj_grp \"" + GRP_INI + \"" adwsor /ADW/\"" + loadType2 + \""/\"" + sub + \""\
\""
+ \""$global_table_info 1\
\""
+ \""#db_nm\u003d\\\""SOR\\\""\
\""
+ \""!nm\u003d\\\""\"" + tblID + \""\\\""\
\""
+ \"".desc1\u003d\\\""\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".up_dt\u003d\\\""\"" + CR_DT +\""\\\""\
\""
+\"".owner_nm \u003d \\\""STGOWN\\\""\
\""
+ \"".ifiller1\u003d\\\""0\\\""\
\""
+ \"".ifiller2\u003d\\\""0\\\""\
\""
+ \"".ifiller3\u003d\\\""0\\\""\
\""
+ \"".sfiller1\u003dNULL\
\""
+ \"".sfiller2\u003dNULL\
\""
+ \"".sfiller3\u003dNULL\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""/global_table_info_dtl db_nm\u003ddb_nm AND table_nm\u003dnm\
\""
+ \""$global_talbe_info_dtl 4\
\""
+ arTblInfo
+ \""$$\
\""
+ \""$prj 0\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \""*prj_grp_id\u003d\\\""\"" + GRP_INI + \""\\\""\
\""
+ \""!nm\u003d\\\""\"" + LID + \""\\\""\
\""
+ \"".desc11\u003d\\\""\"" + LIDnm + \""\\\""\
\""
+ \"".cur_ver_id\u003d\\\""2\\\""\
\""
+ \"".owner_nm\u003d\\\""adwsor\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".up_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".perm\u003d\\\""774\\\""\
\""
+ \"".grp_nm\u003d\\\""adwsor\\\""\
\""
+ \"".favorite_yn\u003d\\N\\\""\
\""
+ \"".status_cd\u003d\\\""0\\\""\
\""
+ \"".sms_id\u003d\\\""0\\\""\
\""
+ \"".eamil_id\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$arrow_dtl 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""2\\\""\
\""
+ \"".sequence\u003d\\\""1\\\""\
\""
+ \"".sequence2\u003d\\\""1\\\""\
\""
+ \"".pos_x\u003d\\\""82\\\""\
\""
+ \"".pos_y\u003d\\\""249\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""2\\\""\
\""
+ \"".sequence\u003d\\\""1\\\""\
\""
+ \"".sequence2\u003d\\\""2\\\""\
\""
+ \"".pos_x\u003d\\\""166\\\""\
\""
+ \"".pos_y\u003d\\\""249\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""3\\\""\
\""
+ \"".sequence\u003d\\\""2\\\""\
\""
+ \"".sequence2\u003d\\\""1\\\""\
\""
+ \"".pos_x\u003d\\\""202\\\""\
\""
+ \"".pos_y\u003d\\\""249\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""3\\\""\
\""
+ \"".sequence\u003d\\\""2\\\""\
\""
+ \"".sequence2\u003d\\\""2\\\""\
\""
+ \"".pos_x\u003d\\\""286\\\""\
\""
+ \"".pos_y\u003d\\\""249\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""4\\\""\
\""
+ \"".sequence\u003d\\\""3\\\""\
\""
+ \"".sequence2\u003d\\\""1\\\""\
\""
+ \"".pos_x\u003d\\\""322\\\""\
\""
+ \"".pos_y\u003d\\\""250\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""4\\\""\
\""
+ \"".sequence\u003d\\\""3\\\""\
\""
+ \"".sequence2\u003d\\\""2\\\""\
\""
+ \"".pos_x\u003d\\\""406\\\""\
\""
+ \"".pos_y\u003d\\\""250\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$block 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""2\\\""\
\""
+ \"".nm\u003d\\\""US_0010\\\""\
\""
+ \"".block_kind_cd\u003d\\\""1\\\""\
\""
+ \"".block_status_cd\u003d\\\""1\\\""\
\""
+ \"".parallel\u003d\\\""1\\\""\
\""
+ \"".comp\u003d\\\""N\\\""\
\""
+ \"".pos_x\u003d\\\""30\\\""\
\""
+ \"".pos_y\u003d\\\""230\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDesciniUS0010 + \""\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".dt_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".block_kind_before\u003d\\\""0\\\""\
\""
+ \"".block_kind_after\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""3\\\""\
\""
+ \"".nm\u003d\\\""LD_0020\\\""\
\""
+ \"".block_kind_cd\u003d\\\""9\\\""\
\""
+ \"".block_status_cd\u003d\\\""1\\\""\
\""
+ \"".parallel\u003d\\\""1\\\""\
\""
+ \"".comp\u003d\\\""N\\\""\
\""
+ \"".pos_x\u003d\\\""150\\\""\
\""
+ \"".pos_y\u003d\\\""230\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDesciniLD0020 + \""\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".dt_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".block_kind_before\u003d\\\""0\\\""\
\""
+ \"".block_kind_after\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""4\\\""\
\""
+ \"".nm\u003d\\\""US_0030\\\""\
\""
+ \"".block_kind_cd\u003d\\\""11\\\""\
\""
+ \"".block_status_cd\u003d\\\""1\\\""\
\""
+ \"".parallel\u003d\\\""1\\\""\
\""
+ \"".comp\u003d\\\""N\\\""\
\""
+ \"".pos_x\u003d\\\""270\\\""\
\""
+ \"".pos_y\u003d\\\""230\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDesciniUS0030 + \""\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".dt_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".block_kind_before\u003d\\\""0\\\""\
\""
+ \"".block_kind_after\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""5\\\""\
\""
+ \"".nm\u003d\\\""US_0040\\\""\
\""
+ \"".block_kind_cd\u003d\\\""11\\\""\
\""
+ \"".block_status_cd\u003d\\\""1\\\""\
\""
+ \"".parallel\u003d\\\""1\\\""\
\""
+ \"".comp\u003d\\\""N\\\""\
\""
+ \"".pos_x\u003d\\\""390\\\""\
\""
+ \"".pos_y\u003d\\\""230\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDesciniUS0040 + \""\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".dt_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".block_kind_before\u003d\\\""0\\\""\
\""
+ \"".block_kind_after\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$dep 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""2\\\""\
\""
+ \"".sequence\u003d\\\""1\\\""\
\""
+ \"".dep_type_cd\u003d\\\""1\\\""\
\""
+ \"".target_block_id\u003d\\\""3\\\""\
\""
+ \"".link_kind_cd\u003d\\\""1\\\""\
\""
+ \"".link_type_cd\u003d\\\""1\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""3\\\""\
\""
+ \"".sequence\u003d\\\""2\\\""\
\""
+ \"".dep_type_cd\u003d\\\""1\\\""\
\""
+ \"".target_block_id\u003d\\\""4\\\""\
\""
+ \"".link_kind_cd\u003d\\\""1\\\""\
\""
+ \"".link_type_cd\u003d\\\""1\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""4\\\""\
\""
+ \"".sequence\u003d\\\""3\\\""\
\""
+ \"".dep_type_cd\u003d\\\""1\\\""\
\""
+ \"".target_block_id\u003d\\\""5\\\""\
\""
+ \"".link_kind_cd\u003d\\\""1\\\""\
\""
+ \"".link_type_cd\u003d\\\""1\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$loading2 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""3\\\""\
\""
+ \"".db_nm\u003d\\\""SOR\\\""\
\""
+ \"".db_type_cd\u003d\\\""2\\\""\
\""
+ \"".native_dsn\u003d\\\""\\\""\
\""
+ \"".user_db_nm\u003d\\\""\\\""\
\""
+ \"".user_nm\u003d\\\""\\\""\
\""
+ \"".passwd\u003d\\\""\\\""\
\""
+ \"".log_file\u003d\\\""$$[SOR_\"" + loadType2 + \""_LOG]/\"" + LID + \"".log\\\""\
\""
+ \"".bad_file\u003d\\\""$$[SOR_\"" + loadType2 + \""_BAD]/\"" + LID + \"".bad\\\""\
\""
+ \"".data_file\u003d\\\""$$[SOR_\"" + loadType2 + \""_SAM]/\"" + sub+ \""/\"" + tblID + \"".dat\\\""\
\""
+ \"".target_table\u003d\\\""\\\""\
\""
+ \"".options\u003d\\\""DIRECT\u003dTRUE ERRORS\u003d0\\\""\
\""
+ \"".ffd_id\u003d\\\""1\\\""\
\""
+ \"".date_format\u003d\\\""YYYYMMDD\\\""\
\""
+ \"".connect_type\u003d\\\""1\\\""\
\""
+ \"".ctl_file\u003d\\\""\"" + siniLD_0020 + \""\\\""\
\""
+ \"".table_ffd_id\u003d\\\""1\\\""\
\""
+ \"".load_type_cd\u003d\\\""1\\\""\
\""
+ \"".ifiller1\u003d\\\""0\\\""\
\""
+ \"".ifiller2\u003d\\\""0\\\""\
\""
+ \"".ifiller3\u003d\\\""0\\\""\
\""
+ \"".sfiller1\u003d\\\""\\\""\
\""
+ \"".sfiller2\u003d\\\""\\\""\
\""
+ \"".sfiller3\u003d\\\""YYYYMMDDHH24MISSFF6\\\""\
\""
+ \"".ifiller4\u003d\\\""0\\\""\
\""
+ \"".ifiller5\u003d\\\""0\\\""\
\""
+ \"".ifiller6\u003d\\\""0\\\""\
\""
+ \"".ifiller7\u003d\\\""0\\\""\
\""
+ \"".ifiller8\u003d\\\""0\\\""\
\""
+ \"".ifiller9\u003d\\\""0\\\""\
\""
+ \"".ifiller10\u003d\\\""0\\\""\
\""
+ \"".sfiller4\u003d\\\""\\\""\
\""
+ \"".sfiller5\u003d\\\""\\\""\
\""
+ \"".sfiller6\u003d\\\""\\\""\
\""
+ \"".sfiller7\u003d\\\""\\\""\
\""
+ \"".sfiller8\u003d\\\""\\\""\
\""
+ \"".sfiller9\u003d\\\""0|\\\""\
\""
+ \"".sfiller10\u003d\\\""\\\""\
\""
+ \"".data_type_cd\u003d\\\""1\\\""\
\""
+ \"".dfs_nm\u003d\\\""LOCAL\\\""\
\""
+ \"".max_errors\u003d\\\""100\\\""\
\""
+ \"".keep_null\u003d\\\""0\\\""\
\""
+ \"".table_level_lock\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$procedure_block 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""2\\\""\
\""
+ \"".db_nm\u003d\\\""SOR\\\""\
\""
+ \"".nm\u003d\\\""US_0010\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDesciniUS0010 + \""\\\""\
\""
+ \"".sql_source\u003d\\\""set serveroutput on]n\""
+ \"".Declare\
\""
+ \"".       o_result int;\
\""
+ \"".       o_errmsg varchar2(4000);\
\""
+ \""\
\""
+ \""BEGIN\
\""
+ siniUS_0010 + \""\
\""
+ \""     dbms_output.put_line(\u0027o_result : \u0027|| o_result);\
\""
+ \""     dbms_output.put_line(\u0027o_result : \u0027|| o_errmsg);\
\""
+ \"".END;\
\""
+\""\\\""\
\""
+ \"".sp_sql\u003d\\\""1\\\""\
\""
+ \"".sp1\u003d\\\""0\\\""\
\""
+ \"".sp2\u003d\\\""1\\\""\
\""
+ \"".sp3\u003d\\\""0\\\""\
\""
+ \"".sp4\u003d\\\""0\\\""\
\""
+ \"".sp_text\u003d\\\""\"" + siniUS_0010 + \""\\\""\
\""
+ \"".sybase_native_dsn\u003d\\\""\\\""\
\""
+ \"".sp_option\u003d\\\""\\\""\
\""
+ \"".ifiller1\u003d\\\""1\\\""\
\""
+ \"".ifiller2\u003d\\\""0\\\""\
\""
+ \"".ifiller3\u003d\\\""0\\\""\
\""
+ \"".sfiller1\u003d\\\""\\\""\
\""
+ \"".sfiller2\u003d\\\""\\\""\
\""
+ \"".sfiller3\u003dNULL\
\""
+ \"".stdout_file\u003dNULL\
\""
+ \"".stderr_file\u003dNULL\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""4\\\""\
\""
+ \"".db_nm\u003d\\\""SOR\\\""\
\""
+ \"".nm\u003d\\\""US_0030\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDesciniUS0030 + \""\\\""\
\""
+ \"".sql_source\u003d\\\""set serveroutput on]n\""
+ \"".Declare\
\""
+ \"".       o_result int;\
\""
+ \"".       o_errmsg varchar2(4000);\
\""
+ \"".       o_count int;\
\""
+ \""\
\""
+ \""BEGIN\
\""
+ siniUS_0030 + \""\
\""
+ \""     dbms_output.put_line(\u0027o_result : \u0027|| o_result);\
\""
+ \""     dbms_output.put_line(\u0027o_result : \u0027|| o_errmsg);\
\""
+ \""     dbms_output.put_line(\u0027o_count : \u0027|| o_count);\
\""
+ \"".END;\
\""
+\""\\\""\
\""
+ \"".sp_sql\u003d\\\""1\\\""\
\""
+ \"".sp1\u003d\\\""0\\\""\
\""
+ \"".sp2\u003d\\\""1\\\""\
\""
+ \"".sp3\u003d\\\""1\\\""\
\""
+ \"".sp4\u003d\\\""0\\\""\
\""
+ \"".sp_text\u003d\\\""\"" + siniUS_0030 + \""\\\""\
\""
+ \"".sybase_native_dsn\u003d\\\""\\\""\
\""
+ \"".sp_option\u003d\\\""\\\""\
\""
+ \"".ifiller1\u003d\\\""1\\\""\
\""
+ \"".ifiller2\u003d\\\""0\\\""\
\""
+ \"".ifiller3\u003d\\\""0\\\""\
\""
+ \"".sfiller1\u003d\\\""\\\""\
\""
+ \"".sfiller2\u003d\\\""\\\""\
\""
+ \"".sfiller3\u003dNULL\
\""
+ \"".stdout_file\u003dNULL\
\""
+ \"".stderr_file\u003dNULL\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""5\\\""\
\""
+ \"".db_nm\u003d\\\""SOR\\\""\
\""
+ \"".nm\u003d\\\""US_0040\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDesciniUS0040 + \""\\\""\
\""
+ \"".sql_source\u003d\\\""set serveroutput on]n\""
+ \"".Declare\
\""
+ \"".       o_result int;\
\""
+ \"".       o_errmsg varchar2(4000);\
\""
+ \"".       o_count int;\
\""
+ \""\
\""
+ \""BEGIN\
\""
+ siniUS_0040 + \""\
\""
+ \""     dbms_output.put_line(\u0027o_result : \u0027|| o_result);\
\""
+ \""     dbms_output.put_line(\u0027o_result : \u0027|| o_errmsg);\
\""
+ \""     dbms_output.put_line(\u0027o_count : \u0027|| o_count);\
\""
+ \"".END;\
\""
+\""\\\""\
\""
+ \"".sp_sql\u003d\\\""1\\\""\
\""
+ \"".sp1\u003d\\\""0\\\""\
\""
+ \"".sp2\u003d\\\""1\\\""\
\""
+ \"".sp3\u003d\\\""1\\\""\
\""
+ \"".sp4\u003d\\\""0\\\""\
\""
+ \"".sp_text\u003d\\\""\"" + siniUS_0040 + \""\\\""\
\""
+ \"".sybase_native_dsn\u003d\\\""\\\""\
\""
+ \"".sp_option\u003d\\\""\\\""\
\""
+ \"".ifiller1\u003d\\\""1\\\""\
\""
+ \"".ifiller2\u003d\\\""0\\\""\
\""
+ \"".ifiller3\u003d\\\""0\\\""\
\""
+ \"".sfiller1\u003d\\\""\\\""\
\""
+ \"".sfiller2\u003d\\\""\\\""\
\""
+ \"".sfiller3\u003dNULL\
\""
+ \"".stdout_file\u003dNULL\
\""
+ \"".stderr_file\u003dNULL\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$prj_ber 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".ver_nm\u003d\\\""\\\""\
\""
+ \"".desc1\u003d\\\""\u003cviewsize\u003e1000;1000\u003c/viewsize\u003e\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".up_dt\u003d\\\""\"" + CR_DT + \\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$note_memo 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""1\\\""\
\""
+ \"".nm\u003d\\\""프로젝트 정보\\\""\
\""
+ \"".block_kind_cd\u003d\\\""13\\\""\
\""
+ \"".block_status_cd\u003d\\\""1\\\""\
\""
+ \"".pos_x\u003d\\\""787404\\\""\
\""
+ \"".pos_y\u003d\\\""786610\\\""\
\""
+ \"".desc1\u003d\\\""bsEllipse\
\""
+ \""1. 프로젝트 명 : \"" + LID + \"" - \"" + LIDnm + \""\
\""
+ \""2. 변경이력\
\""
+ \""     일자                      작성자                 변경내용\
\""
+ \""     ---------------  -------------   -----------------------------------------------------------------------------------------------
+ \""     2020-09-09    ADW                   최초작성\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".up_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".block_kind_before\u003d\\\""0\\\""\
\""
+ \"".block_kind_after\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$global_ffd 1\
\""
+ \""^ffd_id\u003d\\\""1\\\""\
\""
+ \"".col_type_cd\u003d\\\""2\\\""\
\""
+ \"".length\u003d\\\""0\\\""\
\""
+ \"".num_col\u003d\\\""\"" + cbsrowcount + \""\\\""\
\""
+ \""!nm\u003d\\\""\"" + tblID + \""\\\""\
\""
+ \"".desc1\u003d\\\""\"" + tblname + \""\\\""\
\""
+ \"".cr_date\u003d\\\""2020-09-09 17:50:44\\\""\
\""
+ \"".du_date\u003d\\\""2020-09-09 18:19:57\\\""\
\""
+ \"".owner_nm\u003d\\\""adwsor\\\""\
\""
+ \""*ffd_grp_id\u003d\\\""1\\\""\
\""
+ \"".db_nm\u003d\\\""SOR\\\""\
\""
+ \"".table_nm\u003d\\\""\"" + tblID + \""\\\""\
\""
+ \"".ffd_type\u003d\\\""5\\\""\
\""
+ \"".perm\u003d\\\""664\\\""\
\""
+ \"".grp_nm\u003d\\\""adwsor\\\""\
\""
+ \"".ifiller1\u003d\\\""1\\\""\
\""
+ \"".ifiller2\u003d\\\""0\\\""\
\""
+ \"".ifiller3\u003d\\\""0\\\""\
\""
+ \"".sfiller1\u003d\\\""/DBFFD/ADW\\\""\
\""
+ \"".sfiller2\u003dNULL\
\""
+ \"".sfiller3\u003d\\\""STGOWN\\\""\
\""
+ \"".status_cd\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""/global_ffd_dtl ffd_id\u003dffd_id\
\""
+ \""$global_ffd_dtl 4\
\""
+ arFFDInfo
+ \""$$\
\""
+ \""\"";


String sTsCHG \u003d\""file\u003dtsscm1.2\
\""
+ \""/ffd_grp 1 adwsor /DBFFD/ADW\
\""
+ \""/prj_grp \"" + GRP_CHG + \"" adwsor /ADW/\"" + loadType + \""/\"" + sub + \""\
\""
+ \""$global_table_info 1\
\""
+ \""#db_nm\u003d\\\""SOR\\\""\
\""
+ \""!nm\u003d\\\""\"" + tblID + \""\\\""\
\""
+ \"".desc1\u003d\\\""\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".up_dt\u003d\\\""\"" + CR_DT +\""\\\""\
\""
+\"".owner_nm \u003d \\\""STGOWN\\\""\
\""
+ \"".ifiller1\u003d\\\""0\\\""\
\""
+ \"".ifiller2\u003d\\\""0\\\""\
\""
+ \"".ifiller3\u003d\\\""0\\\""\
\""
+ \"".sfiller1\u003dNULL\
\""
+ \"".sfiller2\u003dNULL\
\""
+ \"".sfiller3\u003dNULL\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""/global_table_info_dtl db_nm\u003ddb_nm AND table_nm\u003dnm\
\""
+ \""$global_talbe_info_dtl 4\
\""
+ arTblInfo
+ \""$$\
\""
+ \""$prj 0\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \""*prj_grp_id\u003d\\\""\"" + GRP_CHG + \""\\\""\
\""
+ \""!nm\u003d\\\""\"" + LCD + \""\\\""\
\""
+ \"".desc11\u003d\\\""\"" + LCDnm + \""\\\""\
\""
+ \"".cur_ver_id\u003d\\\""2\\\""\
\""
+ \"".owner_nm\u003d\\\""adwsor\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".up_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".perm\u003d\\\""774\\\""\
\""
+ \"".grp_nm\u003d\\\""adwsor\\\""\
\""
+ \"".favorite_yn\u003d\\N\\\""\
\""
+ \"".status_cd\u003d\\\""0\\\""\
\""
+ \"".sms_id\u003d\\\""0\\\""\
\""
+ \"".eamil_id\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$arrow_dtl 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""2\\\""\
\""
+ \"".sequence\u003d\\\""1\\\""\
\""
+ \"".sequence2\u003d\\\""1\\\""\
\""
+ \"".pos_x\u003d\\\""82\\\""\
\""
+ \"".pos_y\u003d\\\""249\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""2\\\""\
\""
+ \"".sequence\u003d\\\""1\\\""\
\""
+ \"".sequence2\u003d\\\""2\\\""\
\""
+ \"".pos_x\u003d\\\""166\\\""\
\""
+ \"".pos_y\u003d\\\""249\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""3\\\""\
\""
+ \"".sequence\u003d\\\""2\\\""\
\""
+ \"".sequence2\u003d\\\""1\\\""\
\""
+ \"".pos_x\u003d\\\""202\\\""\
\""
+ \"".pos_y\u003d\\\""249\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""3\\\""\
\""
+ \"".sequence\u003d\\\""2\\\""\
\""
+ \"".sequence2\u003d\\\""2\\\""\
\""
+ \"".pos_x\u003"
"d\\\""286\\\""\
\""
+ \"".pos_y\u003d\\\""249\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""4\\\""\
\""
+ \"".sequence\u003d\\\""3\\\""\
\""
+ \"".sequence2\u003d\\\""1\\\""\
\""
+ \"".pos_x\u003d\\\""322\\\""\
\""
+ \"".pos_y\u003d\\\""250\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""4\\\""\
\""
+ \"".sequence\u003d\\\""3\\\""\
\""
+ \"".sequence2\u003d\\\""2\\\""\
\""
+ \"".pos_x\u003d\\\""406\\\""\
\""
+ \"".pos_y\u003d\\\""250\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$block 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""2\\\""\
\""
+ \"".nm\u003d\\\""US_0010\\\""\
\""
+ \"".block_kind_cd\u003d\\\""1\\\""\
\""
+ \"".block_status_cd\u003d\\\""1\\\""\
\""
+ \"".parallel\u003d\\\""1\\\""\
\""
+ \"".comp\u003d\\\""N\\\""\
\""
+ \"".pos_x\u003d\\\""30\\\""\
\""
+ \"".pos_y\u003d\\\""230\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDescchgUS0010 + \""\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".dt_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".block_kind_before\u003d\\\""0\\\""\
\""
+ \"".block_kind_after\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""3\\\""\
\""
+ \"".nm\u003d\\\""LD_0020\\\""\
\""
+ \"".block_kind_cd\u003d\\\""9\\\""\
\""
+ \"".block_status_cd\u003d\\\""1\\\""\
\""
+ \"".parallel\u003d\\\""1\\\""\
\""
+ \"".comp\u003d\\\""N\\\""\
\""
+ \"".pos_x\u003d\\\""150\\\""\
\""
+ \"".pos_y\u003d\\\""230\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDescchgLD0020 + \""\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".dt_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".block_kind_before\u003d\\\""0\\\""\
\""
+ \"".block_kind_after\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""4\\\""\
\""
+ \"".nm\u003d\\\""US_0030\\\""\
\""
+ \"".block_kind_cd\u003d\\\""11\\\""\
\""
+ \"".block_status_cd\u003d\\\""1\\\""\
\""
+ \"".parallel\u003d\\\""1\\\""\
\""
+ \"".comp\u003d\\\""N\\\""\
\""
+ \"".pos_x\u003d\\\""270\\\""\
\""
+ \"".pos_y\u003d\\\""230\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDescchgUS0030 + \""\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".dt_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".block_kind_before\u003d\\\""0\\\""\
\""
+ \"".block_kind_after\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""5\\\""\
\""
+ \"".nm\u003d\\\""US_0040\\\""\
\""
+ \"".block_kind_cd\u003d\\\""11\\\""\
\""
+ \"".block_status_cd\u003d\\\""1\\\""\
\""
+ \"".parallel\u003d\\\""1\\\""\
\""
+ \"".comp\u003d\\\""N\\\""\
\""
+ \"".pos_x\u003d\\\""390\\\""\
\""
+ \"".pos_y\u003d\\\""230\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDescchgUS0040 + \""\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".dt_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".block_kind_before\u003d\\\""0\\\""\
\""
+ \"".block_kind_after\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$dep 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""2\\\""\
\""
+ \"".sequence\u003d\\\""1\\\""\
\""
+ \"".dep_type_cd\u003d\\\""1\\\""\
\""
+ \"".target_block_id\u003d\\\""3\\\""\
\""
+ \"".link_kind_cd\u003d\\\""1\\\""\
\""
+ \"".link_type_cd\u003d\\\""1\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""3\\\""\
\""
+ \"".sequence\u003d\\\""2\\\""\
\""
+ \"".dep_type_cd\u003d\\\""1\\\""\
\""
+ \"".target_block_id\u003d\\\""4\\\""\
\""
+ \"".link_kind_cd\u003d\\\""1\\\""\
\""
+ \"".link_type_cd\u003d\\\""1\\\""\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""4\\\""\
\""
+ \"".sequence\u003d\\\""3\\\""\
\""
+ \"".dep_type_cd\u003d\\\""1\\\""\
\""
+ \"".target_block_id\u003d\\\""5\\\""\
\""
+ \"".link_kind_cd\u003d\\\""1\\\""\
\""
+ \"".link_type_cd\u003d\\\""1\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$loading2 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""3\\\""\
\""
+ \"".db_nm\u003d\\\""SOR\\\""\
\""
+ \"".db_type_cd\u003d\\\""2\\\""\
\""
+ \"".native_dsn\u003d\\\""\\\""\
\""
+ \"".user_db_nm\u003d\\\""\\\""\
\""
+ \"".user_nm\u003d\\\""\\\""\
\""
+ \"".passwd\u003d\\\""\\\""\
\""
+ \"".log_file\u003d\\\""$$[SOR_\"" + loadType + \""_LOG]/\"" + LCD + \"".log\\\""\
\""
+ \"".bad_file\u003d\\\""$$[SOR_\"" + loadType + \""_BAD]/\"" + LCD + \"".bad\\\""\
\""
+ \"".data_file\u003d\\\""$$[SOR_\"" + loadType + \""_SAM]/\"" + sub+ \""/\"" + tblID + \"".dat\\\""\
\""
+ \"".target_table\u003d\\\""\\\""\
\""
+ \"".options\u003d\\\""DIRECT\u003dTRUE ERRORS\u003d0\\\""\
\""
+ \"".ffd_id\u003d\\\""1\\\""\
\""
+ \"".date_format\u003d\\\""YYYYMMDD\\\""\
\""
+ \"".connect_type\u003d\\\""1\\\""\
\""
+ \"".ctl_file\u003d\\\""\"" + schgLD_0020 + \""\\\""\
\""
+ \"".table_ffd_id\u003d\\\""1\\\""\
\""
+ \"".load_type_cd\u003d\\\""1\\\""\
\""
+ \"".ifiller1\u003d\\\""0\\\""\
\""
+ \"".ifiller2\u003d\\\""0\\\""\
\""
+ \"".ifiller3\u003d\\\""0\\\""\
\""
+ \"".sfiller1\u003d\\\""\\\""\
\""
+ \"".sfiller2\u003d\\\""\\\""\
\""
+ \"".sfiller3\u003d\\\""YYYYMMDDHH24MISSFF6\\\""\
\""
+ \"".ifiller4\u003d\\\""0\\\""\
\""
+ \"".ifiller5\u003d\\\""0\\\""\
\""
+ \"".ifiller6\u003d\\\""0\\\""\
\""
+ \"".ifiller7\u003d\\\""0\\\""\
\""
+ \"".ifiller8\u003d\\\""0\\\""\
\""
+ \"".ifiller9\u003d\\\""0\\\""\
\""
+ \"".ifiller10\u003d\\\""0\\\""\
\""
+ \"".sfiller4\u003d\\\""\\\""\
\""
+ \"".sfiller5\u003d\\\""\\\""\
\""
+ \"".sfiller6\u003d\\\""\\\""\
\""
+ \"".sfiller7\u003d\\\""\\\""\
\""
+ \"".sfiller8\u003d\\\""\\\""\
\""
+ \"".sfiller9\u003d\\\""0|\\\""\
\""
+ \"".sfiller10\u003d\\\""\\\""\
\""
+ \"".data_type_cd\u003d\\\""1\\\""\
\""
+ \"".dfs_nm\u003d\\\""LOCAL\\\""\
\""
+ \"".max_errors\u003d\\\""100\\\""\
\""
+ \"".keep_null\u003d\\\""0\\\""\
\""
+ \"".table_level_lock\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$procedure_block 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""2\\\""\
\""
+ \"".db_nm\u003d\\\""SOR\\\""\
\""
+ \"".nm\u003d\\\""US_0010\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDescchgUS0010 + \""\\\""\
\""
+ \"".sql_source\u003d\\\""set serveroutput on]n\""
+ \"".Declare\
\""
+ \"".       o_result int;\
\""
+ \"".       o_errmsg varchar2(4000);\
\""
+ \""\
\""
+ \""BEGIN\
\""
+ schgUS_0010 + \""\
\""
+ \""     dbms_output.put_line(\u0027o_result : \u0027|| o_result);\
\""
+ \""     dbms_output.put_line(\u0027o_result : \u0027|| o_errmsg);\
\""
+ \"".END;\
\""
+\""\\\""\
\""
+ \"".sp_sql\u003d\\\""1\\\""\
\""
+ \"".sp1\u003d\\\""0\\\""\
\""
+ \"".sp2\u003d\\\""1\\\""\
\""
+ \"".sp3\u003d\\\""0\\\""\
\""
+ \"".sp4\u003d\\\""0\\\""\
\""
+ \"".sp_text\u003d\\\""\"" + schgUS_0010 + \""\\\""\
\""
+ \"".sybase_native_dsn\u003d\\\""\\\""\
\""
+ \"".sp_option\u003d\\\""\\\""\
\""
+ \"".ifiller1\u003d\\\""1\\\""\
\""
+ \"".ifiller2\u003d\\\""0\\\""\
\""
+ \"".ifiller3\u003d\\\""0\\\""\
\""
+ \"".sfiller1\u003d\\\""\\\""\
\""
+ \"".sfiller2\u003d\\\""\\\""\
\""
+ \"".sfiller3\u003dNULL\
\""
+ \"".stdout_file\u003dNULL\
\""
+ \"".stderr_file\u003dNULL\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""4\\\""\
\""
+ \"".db_nm\u003d\\\""SOR\\\""\
\""
+ \"".nm\u003d\\\""US_0030\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDescchgUS0030 + \""\\\""\
\""
+ \"".sql_source\u003d\\\""set serveroutput on]n\""
+ \"".Declare\
\""
+ \"".       o_result int;\
\""
+ \"".       o_errmsg varchar2(4000);\
\""
+ \"".       o_count int;\
\""
+ \""\
\""
+ \""BEGIN\
\""
+ schgUS_0030 + \""\
\""
+ \""     dbms_output.put_line(\u0027o_result : \u0027|| o_result);\
\""
+ \""     dbms_output.put_line(\u0027o_result : \u0027|| o_errmsg);\
\""
+ \""     dbms_output.put_line(\u0027o_count : \u0027|| o_count);\
\""
+ \"".END;\
\""
+\""\\\""\
\""
+ \"".sp_sql\u003d\\\""1\\\""\
\""
+ \"".sp1\u003d\\\""0\\\""\
\""
+ \"".sp2\u003d\\\""1\\\""\
\""
+ \"".sp3\u003d\\\""1\\\""\
\""
+ \"".sp4\u003d\\\""0\\\""\
\""
+ \"".sp_text\u003d\\\""\"" + schgUS_0030 + \""\\\""\
\""
+ \"".sybase_native_dsn\u003d\\\""\\\""\
\""
+ \"".sp_option\u003d\\\""\\\""\
\""
+ \"".ifiller1\u003d\\\""1\\\""\
\""
+ \"".ifiller2\u003d\\\""0\\\""\
\""
+ \"".ifiller3\u003d\\\""0\\\""\
\""
+ \"".sfiller1\u003d\\\""\\\""\
\""
+ \"".sfiller2\u003d\\\""\\\""\
\""
+ \"".sfiller3\u003dNULL\
\""
+ \"".stdout_file\u003dNULL\
\""
+ \"".stderr_file\u003dNULL\
\""
+ \""$+\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""5\\\""\
\""
+ \"".db_nm\u003d\\\""SOR\\\""\
\""
+ \"".nm\u003d\\\""US_0040\\\""\
\""
+ \"".desc1\u003d\\\""\"" + sDescchgUS0040 + \""\\\""\
\""
+ \"".sql_source\u003d\\\""set serveroutput on]n\""
+ \"".Declare\
\""
+ \"".       o_result int;\
\""
+ \"".       o_errmsg varchar2(4000);\
\""
+ \"".       o_count int;\
\""
+ \""\
\""
+ \""BEGIN\
\""
+ schgUS_0040 + \""\
\""
+ \""     dbms_output.put_line(\u0027o_result : \u0027|| o_result);\
\""
+ \""     dbms_output.put_line(\u0027o_result : \u0027|| o_errmsg);\
\""
+ \""     dbms_output.put_line(\u0027o_count : \u0027|| o_count);\
\""
+ \"".END;\
\""
+\""\\\""\
\""
+ \"".sp_sql\u003d\\\""1\\\""\
\""
+ \"".sp1\u003d\\\""0\\\""\
\""
+ \"".sp2\u003d\\\""1\\\""\
\""
+ \"".sp3\u003d\\\""1\\\""\
\""
+ \"".sp4\u003d\\\""0\\\""\
\""
+ \"".sp_text\u003d\\\""\"" + schgUS_0040 + \""\\\""\
\""
+ \"".sybase_native_dsn\u003d\\\""\\\""\
\""
+ \"".sp_option\u003d\\\""\\\""\
\""
+ \"".ifiller1\u003d\\\""1\\\""\
\""
+ \"".ifiller2\u003d\\\""0\\\""\
\""
+ \"".ifiller3\u003d\\\""0\\\""\
\""
+ \"".sfiller1\u003d\\\""\\\""\
\""
+ \"".sfiller2\u003d\\\""\\\""\
\""
+ \"".sfiller3\u003dNULL\
\""
+ \"".stdout_file\u003dNULL\
\""
+ \"".stderr_file\u003dNULL\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$prj_ber 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".ver_nm\u003d\\\""\\\""\
\""
+ \"".desc1\u003d\\\""\u003cviewsize\u003e1000;1000\u003c/viewsize\u003e\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".up_dt\u003d\\\""\"" + CR_DT + \\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$note_memo 4\
\""
+ \"".prj_id\u003d\\\""1\\\""\
\""
+ \"".prj_ver_id\u003d\\\""1\\\""\
\""
+ \"".block_id\u003d\\\""1\\\""\
\""
+ \"".nm\u003d\\\""프로젝트 정보\\\""\
\""
+ \"".block_kind_cd\u003d\\\""13\\\""\
\""
+ \"".block_status_cd\u003d\\\""1\\\""\
\""
+ \"".pos_x\u003d\\\""787404\\\""\
\""
+ \"".pos_y\u003d\\\""786610\\\""\
\""
+ \"".desc1\u003d\\\""bsEllipse\
\""
+ \""1. 프로젝트 명 : \"" + LCD + \"" - \"" + LCDnm + \""\
\""
+ \""2. 변경이력\
\""
+ \""     일자                      작성자                 변경내용\
\""
+ \""     ---------------  -------------   -----------------------------------------------------------------------------------------------
+ \""     2020-09-09    ADW                   최초작성\\\""\
\""
+ \"".cr_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".up_dt\u003d\\\""\"" + CR_DT + \""\\\""\
\""
+ \"".block_kind_before\u003d\\\""0\\\""\
\""
+ \"".block_kind_after\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""$global_ffd 1\
\""
+ \""^ffd_id\u003d\\\""1\\\""\
\""
+ \"".col_type_cd\u003d\\\""2\\\""\
\""
+ \"".length\u003d\\\""0\\\""\
\""
+ \"".num_col\u003d\\\""\"" + cbsrowcount + \""\\\""\
\""
+ \""!nm\u003d\\\""\"" + tblID + \""\\\""\
\""
+ \"".desc1\u003d\\\""\"" + tblname + \""\\\""\
\""
+ \"".cr_date\u003d\\\""2020-09-09 17:50:44\\\""\
\""
+ \"".du_date\u003d\\\""2020-09-09 18:19:57\\\""\
\""
+ \"".owner_nm\u003d\\\""adwsor\\\""\
\""
+ \""*ffd_grp_id\u003d\\\""1\\\""\
\""
+ \"".db_nm\u003d\\\""SOR\\\""\
\""
+ \"".table_nm\u003d\\\""\"" + tblID + \""\\\""\
\""
+ \"".ffd_type\u003d\\\""5\\\""\
\""
+ \"".perm\u003d\\\""664\\\""\
\""
+ \"".grp_nm\u003d\\\""adwsor\\\""\
\""
+ \"".ifiller1\u003d\\\""1\\\""\
\""
+ \"".ifiller2\u003d\\\""0\\\""\
\""
+ \"".ifiller3\u003d\\\""0\\\""\
\""
+ \"".sfiller1\u003d\\\""/DBFFD/ADW\\\""\
\""
+ \"".sfiller2\u003dNULL\
\""
+ \"".sfiller3\u003d\\\""STGOWN\\\""\
\""
+ \"".status_cd\u003d\\\""0\\\""\
\""
+ \""$+\
\""
+ \""$$\
\""
+ \""/global_ffd_dtl ffd_id\u003dffd_id\
\""
+ \""$global_ffd_dtl 4\
\""
+ arFFDInfo
+ \""$$\
\""
+ \""\"";


File TSLIDfile \u003d new File(siniPgmPath);

if (!TSLIDfile.exists()) {

FileOutputStream fileOutStream \u003d new FileOutputStream(siniPgmPath);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sTsINI);
bufferedWriter.close();
chgCount ++;
} else {
FileInputStream fileInStream \u003d new FileInputStream(siniPgmPath);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;
String TSLIDfileoldFile \u003d \""\"";

while((ch \u003d bufferedReader.read()) !\u003d -1) {

TSLIDfileoldFile +\u003d (char)ch;
}

String tsFileOldFileBe \u003d TSLIDfileoldFile.replaceAll(\""202[0-9].{15}\"", \""\"");
String sTsBe \u003d sTsINI.replaceAll(\""202[0-9].{15}\"", \""\"");

if (tsFileOldFileBe.equals(sTsBe)) {

} else if (!tsFileOldFileBe.equals(sTsBe) \u0026\u0026 TSLIDfile.exists()) {
FileOutputStream fileOutStream \u003d new FileOutputStream(siniPgmPath);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sTsINI);
bufferedWriter.close();
chgCount ++;
}
}

File TSLCDfile \u003d new File(schgPgmPath);

if (!TSLCDfile.exists()) {

FileOutputStream fileOutStream \u003d new FileOutputStream(schgPgmPath);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sTsCHG);
bufferedWriter.close();
chgCount ++;
} else {
FileInputStream fileInStream \u003d new FileInputStream(schgPgmPath);
InputStreamWriter inputStreamReader \u003d new InputStreamWriter(fileOutStream, \""UTF8\"");
BufferedReader bufferedReader \u003d new BufferedReader(inputStreamReader);

int ch;
String TSLCDfileoldFile \u003d \""\"";

while((ch \u003d bufferedReader.read()) !\u003d -1) {

TSLCDfileoldFile +\u003d (char)ch;
}

String tsFileOldFileBe \u003d TSLCDfileoldFile.replaceAll(\""202[0-9].{15}\"", \""\"");
String sTsBe \u003d sTsCHG.replaceAll(\""202[0-9].{15}\"", \""\"");

if (tsFileOldFileBe.equals(sTsBe)) {

} else if (!tsFileOldFileBe.equals(sTsBe) \u0026\u0026 TSLCDfile.exists()) {
FileOutputStream fileOutStream \u003d new FileOutputStream(schgPgmPath);
OutputStreamWriter outputStreamReader \u003d new OutputStreamWriter(fileOutStream, \""UTF8\"");
BufferedWriter bufferedWriter \u003d new BufferedWriter(outputStreamReader);
bufferedWriter.write(sTsCHG);
bufferedWriter.close();
chgCount ++;
}
}
}
if (chgCount \u003d\u003d 0) {
System.out.println(\""[\"" + tblno + \""]\"" + \""[\"" + tblID + \""]\"" + \""[\"" + tblname + \""]\"" + \"" [ts파일 변경없음]\"");
notChangeCount++;
} else {
System.out.println(\""[\"" + tblno + \""]\"" + \""[\"" + tblID + \""]\"" + \""[\"" + tblname + \""]\"" + \"" [ts파일 생성완료]\"");
allChangeCount++;
}

}

int allTablecnt \u003d notChangeCount + allChangeCount + ifTableCount + delTableCount;
System.out.println(\""\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\"");
System.out.println(\""총 \"" + delTableCount + \"" 건 테이블 삭제 ]\"");
System.out.println(\""총 \"" + ifTableCount + \"" 건 테이블 I/F적재형 ]\"");
System.out.println(\""총 \"" + notChangeCount + \"" 건 테이블 ts 변경 없음 ]\"");
System.out.println(\""총 \"" + allChangeCount + \"" 건 테이블 ts 생성 작업 완료 ]\"");
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
