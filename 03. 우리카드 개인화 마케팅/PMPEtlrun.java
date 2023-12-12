import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.CallableStatement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import oracle.jdbc.pool.OracleDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PMPEtlrun {

    public static final Logger LOGGER = LoggerFactory.getLogger(PMPBatchrun.class);

    public static void main (String args [])
        throws Exception
    {
        Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();

        Properties propEnv = getEnvProperties();

        Runtime rt = Runtime.getRuntime();

        String logDir = propEnv.getProperty("LOG_DIR");
        String samDir = propEnv.getProperty("SAM_DIR");
        String conDir = propEnv.getProperty("CON_DIR");
        String badDir = propEnv.getProperty("BAD_DIR");
//        String concatFileInfo = samDir+fileName;

        String dbService = "";
        String dbInfo = "";
        String pmpUser = "";
        String pmpPass = "";
        String oriTbName = "";

        int resChk      = 0;
        int createFailCnt   = 0;
        int defaultCnt  = 0;
        int excCount = 0;
        int filecnt = 0;
        int excPartition = 0;
        int sqlLoaderBadChkCnt = 0;

        String errStr   = "";

        String stDate = "";
        String stTime = "";
        String edDate = "";
        String edTime = "";
        String logStartDate = "";
        String logStartTime = "";
        String excCreDate = "";
        String excCreTime = "";
        String chkDate = "";
        String chkTime = "";
        String conCreDate = "";
        String conCreTime = "";
        String excTruncateDate = "";
        String excTruncateTime = "";
        String sqlLoaderDate = "";
        String sqlLoaderTime = "";
        String excCntDate = "";
        String excCntTime = "";
        String excParDate = "";
        String excParTime = "";

        String totLog   = "";
        String conFile   = "";
        String logPath  = "";
        String loadLogPath = "";
        String conPath  = "";

        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        stDate = df.format(calendar.getTime());
        df = new SimpleDateFormat("HHmmss");
        stTime = df.format(calendar.getTime());

        Encrypt encrypt = new Encrypt();
        dbService = propEnv.getProperty("DB_SERVICE");
        dbInfo = propEnv.getProperty("DB_URL");
        pmpUser = propEnv.getProperty("DB_USER_NAME");
        pmpPass = encrypt.decryptAES256(propEnv.getProperty("DB_PASSWORD"));

        OracleDataSource ods = new OracleDataSource();
        ods.setURL(dbInfo);
        ods.setUser(pmpUser);
        ods.setPassword(pmpPass);
        Connection conn = ods.getConnection();
        Statement step = conn.createStatement();

        totLog = "  =============================================================" + "\n";
        totLog += "                         <���α׷� ����>" + "\n";
        totLog += "    �����α׷�       : " + args[1] + "\n";
        totLog += "    ����۽ð�       : " + stDate + stTime + "\n";

        try {
             if(LOGGER.isInfoEnabled()) {
                LOGGER.info("  =============================================================");
                LOGGER.info("                         <���α׷� ����>");
             }
             if (args.length < 2) {
               if(LOGGER.isInfoEnabled()) {
                  LOGGER.info("    ERROR!!");
                  LOGGER.info("    �Է� Parameter�� ��Ȯ���� �ʽ��ϴ�.");
                  LOGGER.info("  =============================================================");
                  LOGGER.info("    TITLE) java Data Loading Program");
                  LOGGER.info("    USAGE) java PMPBatchrun DATE JOBNAME");
                  LOGGER.info("       EX) java PMPBatchrun 20210417 BCD_WCPMI201TM_TG");
                  LOGGER.info("  =============================================================");
                  LOGGER.info("  " + resChk);
               }
             System.exit(1); // ���� ����
             }

             if(LOGGER.isInfoEnabled()) {
                LOGGER.info("    �����α׷���     : " + args[1]);
                LOGGER.info("    ����۽ð�       : " + stDate + stTime);
             }

             logPath = logDir + args[1].substring(4, 9) + "/" + args[0] + "_" + args[1] + ".log";
             loadLogPath = logDir + args[1].substring(4, 9) + "/" + args[0] + "_" + args[1] + "_sqlloader.log";

             // �ߺ� ���� �۾� üũ
             String cmd[] = {"sh", "-c", "ps -ef | grep PMPEtlrun | grep "+ args[1] + " | wc -l "};
             Process procprcchk = rt.exec(cmd);
             procprcchk.waitFor();

             BufferedReader cbr = new BufferedReader(new InputStreamReader(procprcchk.getInputStream()));
             String str = "";

             try {
                 str = cbr.readLine();

                 if( str != null) {
                   int cnt = Integer.parseInt(str);
                   if(cnt > 2) {
                     Connection connChk = null;
                     PreparedStatement pstmtChk = null;
                     ResultSet rsChk = null;

                     try {

                         connChk = ods.getConnection();

                         String jobLogSql = "SELECT /*+ FULL(T10) PARALLEL(4) */ COUNT(1) AS CNT FROM PMPOWN.WCPCM001TH T10 WHERE BAT_WK_PRRST_CD = 'R' AND BAT_WK_ID = '" + args[1] + "' ";
                         pstmtChk = connChk.prepareStatement(jobLogSql);
                         rsChk = pstmtChk.executeQuery();

                         if(rsChk.next()) {
                           int chkCnt = rsChk.getInt("CNT");

                           if (chkCnt > 0) {
                             if(LOGGER.isInfoEnabled()) {
                                 LOGGER.info("    ERROR!!");
                                 LOGGER.info("    �̹� ���� ���� �۾� �Դϴ�.");
                                 LOGGER.info("  =============================================================");
                             }
                             System.exit(1);
                           }
                         }
                       //sql = ""; // initial Sql Query
                     } catch(Exception chke) {
                       if(LOGGER.isErrorEnabled()) LOGGER.error("  " + chke.getMessage());
                     } finally {
                       if(pstmtChk !=null) {try {pstmtChk.close();} catch(Exception conne) {}}
                       if(connChk !=null) {try {connChk.close();} catch(Exception conne) {}}
                     }
                   }
                 }
             } catch (Exception e) {
                 if(LOGGER.isErrorEnabled()) LOGGER.error("  " + e.getMessage());
             } finally {
                 cbr.close();
             }
             
             // BAD ���� ����
             String cmdbebadfilechk[] = {"sh", "-c", "rm -rf /pmp/dwdb/bad/" + args[1].substring(4, 14) + "_" + args[0] + ".bad"};
             Process procprcbebadfilechk = rt.exec(cmdbebadfilechk);
             procprcbebadfilechk.waitFor();

             // SAM ���� ���翩�� üũ
             String cmdfilechk[] = {"sh", "-c", "find /pmp/dwdb/recv/" + args[1].substring(4, 14) + "_" + args[0] + ".dat -type f | wc -l"};
             Process procprcfilechk = rt.exec(cmdfilechk);
             procprcfilechk.waitFor();
             
             BufferedReader cbrfilechk = new BufferedReader(new InputStreamReader(procprcfilechk.getInputStream()));
             String strfilechk = "";
             
             try {
                 strfilechk = cbrfilechk.readLine();
                 if(strfilechk.equals("0")) {
                   LOGGER.info("    ��ERROR      : ������ �������� �ʽ��ϴ�. �۾�����");
                   System.exit(1);
                 } else {
                   // ���� �Ǽ� üũ
                   LOGGER.info("    ������üũ       : Y");
                   String cmdcntchk[] = {"sh", "-c", "du -s /pmp/dwdb/recv/" + args[1].substring(4, 14) + "_" + args[0] + ".dat"};
                   Process procprcfilecntchk = rt.exec(cmdcntchk);
                   procprcfilecntchk.waitFor();
             
                   BufferedReader cbrfilecntchk = new BufferedReader(new InputStreamReader(procprcfilecntchk.getInputStream()));
                   String strfilecntchk = "";

                   try {
                       strfilecntchk = cbrfilecntchk.readLine();
                       String fileSizechk = strfilecntchk.substring(0,1);
                       if(fileSizechk.equals("0")) {
                         LOGGER.info("    �����ϻ�����      : " + strfilecntchk + " �۾� ����");
                         filecnt++;
                         //System.exit(1);
                       } else {
                         LOGGER.info("    �����ϻ�����      : " + strfilecntchk + " �۾� ����");
                       }
             
                   } catch (Exception e) {
                       if(LOGGER.isErrorEnabled()) LOGGER.error("  " + e.getMessage());
                   } finally {
                       cbrfilecntchk.close();
                   }
                 }
             
             } catch (Exception e) {
                 if(LOGGER.isErrorEnabled()) LOGGER.error("  " + e.getMessage());
             } finally {
                 cbrfilechk.close();
             }



             oriTbName = args[1].substring(4, args[1].length() - 3);

             /*-----------------------------------------------------------------------------------*/
             /* �۾����� �α� Procedure ȣ�� ����                                                 */
             /*-----------------------------------------------------------------------------------*/

             String sql = " DECLARE P_RTN           VARCHAR2(10)  :='';";
             sql += " P_RM_NI_SR_DT                 VARCHAR2(16)  := '" + stDate + stTime + "00';";
             sql += " P_RM_SR_DT                    VARCHAR2(16)  := '" + stDate + stTime + "00';";
             sql += " P_ALTER1                      VARCHAR2(1000);";
             sql += " P_ALTER2                      VARCHAR2(1000);";
             sql += " P_PMP_BAT_PGM_NM              VARCHAR2(100) := '';";
             sql += " BEGIN ";
             sql += "     P_ALTER1 := 'ALTER SESSION SET NLS_DATE_FORMAT      = \"YYYYMMDD\"'; ";
             sql += "     P_ALTER2 := 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = \"YYYYMMDDHH24MISSFF2\"'; ";
             sql += "     EXECUTE IMMEDIATE P_ALTER1; ";
             sql += "     EXECUTE IMMEDIATE P_ALTER2; ";
             sql += "     SELECT NVL(MAX(COMMENTS), ' ') || ' ��ġ ���α׷�' INTO P_PMP_BAT_PGM_NM FROM(SELECT COMMENTS FROM ALL_TAB_COMMENTS ";
             sql += "     WHERE OWNER = 'PMPOWN' AND TABLE_NAME = '" + oriTbName + "' UNION ALL SELECT '" + args[1] + "' FROM DUAL) T10; ";
             sql += "     PMPOWN.SP_LOG_MRT('" + args[1] + "',P_PMP_BAT_PGM_NM,'" + args[0]
                 + "','000','���� ������� �α�',P_RM_NI_SR_DT,P_RM_SR_DT,"
                 + "0,' ','','R','N',P_RTN); END;";

             step.executeUpdate(sql);
             
             calendar = Calendar.getInstance();
             df = new SimpleDateFormat("yyyyMMdd");
             logStartDate = df.format(calendar.getTime());
             df = new SimpleDateFormat("HHmmss");
             logStartTime = df.format(calendar.getTime());
               
             if(LOGGER.isErrorEnabled()) {
               LOGGER.info("    ��" + logStartDate + logStartTime + " : ���ʽ��� �۾��α�");
               totLog += "    ��" + logStartDate + logStartTime + " : ���ʽ��� �۾��α�\n";
             }

             if (filecnt == 1) {
               LOGGER.info("    ��JOB SKIP");
             /*-----------------------------------------------------------------------------------*/
             /* �۾��Ϸ� �α� Procedure ȣ�� ����                                                 */
             /*-----------------------------------------------------------------------------------*/

             String endsql = " DECLARE P_RTN           VARCHAR2(10)  :='';";
             endsql += " P_RM_NI_SR_DT                 VARCHAR2(16)  := '" + stDate + stTime + "00';";
             endsql += " P_RM_SR_DT                    VARCHAR2(16)  := '" + stDate + stTime + "00';";
             endsql += " P_ALTER1                      VARCHAR2(1000);";
             endsql += " P_ALTER2                      VARCHAR2(1000);";
             endsql += " P_PMP_BAT_PGM_NM              VARCHAR2(100) := '';";
             endsql += " BEGIN ";
             endsql += "     P_ALTER1 := 'ALTER SESSION SET NLS_DATE_FORMAT      = \"YYYYMMDD\"'; ";
             endsql += "     P_ALTER2 := 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = \"YYYYMMDDHH24MISSFF2\"'; ";
             endsql += "     EXECUTE IMMEDIATE P_ALTER1; ";
             endsql += "     EXECUTE IMMEDIATE P_ALTER2; ";
             endsql += "     SELECT NVL(MAX(COMMENTS), ' ') || ' ��ġ ���α׷�' INTO P_PMP_BAT_PGM_NM FROM(SELECT COMMENTS FROM ALL_TAB_COMMENTS ";
             endsql += "     WHERE OWNER = 'PMPOWN' AND TABLE_NAME = '" + oriTbName + "' UNION ALL SELECT '" + args[1] + "' FROM DUAL) T10; ";
             endsql += "     PMPOWN.SP_LOG_MRT('" + args[1] + "',P_PMP_BAT_PGM_NM,'" + args[0]
                 + "','000','�۾� �Ϸ�',P_RM_NI_SR_DT,P_RM_SR_DT,"
                 + "0,' ','','S','Y',P_RTN); END;";

             step.executeUpdate(endsql);
             } else {
               /*------------------------------------------------------------------------------*/
               /* EXCHANG TABLE CREATE                                                         */
               /*------------------------------------------------------------------------------*/
               String sqlExcr = " DECLARE P_RTN     VARCHAR2(10) := ''; ";
               sqlExcr += " BEGIN ";
               sqlExcr += " PMPOWN.SP_EXC_TABLE_CR(P_RTN, 'PMPOWN', '" + oriTbName + "'); END;";

               step.executeUpdate(sqlExcr);

               Connection conCheck = null;
               PreparedStatement psCheck = null;
               ResultSet resCheck = null;

               try {

                   conCheck = ods.getConnection();

                   String creynSql = "SELECT /*+ FULL(T10) PARALLEL(4) */ COUNT(1) AS CNT FROM ALL_TABLES T10 WHERE TABLE_NAME = 'EXC_" + oriTbName + "' ";
                   psCheck = conCheck.prepareStatement(creynSql);
                   resCheck = psCheck.executeQuery();
                   
                   calendar = Calendar.getInstance();
                   df = new SimpleDateFormat("yyyyMMdd");
                   excCreDate = df.format(calendar.getTime());
                   df = new SimpleDateFormat("HHmmss");
                   excCreTime = df.format(calendar.getTime());

                   if(resCheck.next()) {
                     int checkCnt = resCheck.getInt("CNT");
                     String sqlCreate = "";

                     /*-----------------------------------------------------------------------------------*/
                     /* EXC ���̺� �������� �α� INSERT                                                   */
                     /*-----------------------------------------------------------------------------------*/

                     if (checkCnt == 0) {
                       if(LOGGER.isInfoEnabled()) {
                           LOGGER.info("    ��" + excCreDate + excCreTime + " : EXC ���̺� ���� ����!");
                           totLog += "    ��" + excCreDate + excCreTime + " : EXC ���̺� ���� ����!\n";
                           //LOGGER.info("  =============================================================");

                           sqlCreate += " DECLARE P_RTN           VARCHAR2(10)  :='';";
                           sqlCreate += " P_RM_NI_SR_DT           VARCHAR2(16)  := '" + stDate + stTime + "00';";
                           sqlCreate += " P_RM_SR_DT              VARCHAR2(16)  := '" + stDate + stTime + "00';";
                           sqlCreate += " P_ALTER1                VARCHAR2(1000);";
                           sqlCreate += " P_ALTER2                VARCHAR2(1000);";
                           sqlCreate += " P_PMP_BAT_PGM_NM        VARCHAR2(100) := '';";
                           sqlCreate += " BEGIN ";
                           sqlCreate += "     P_ALTER1 := 'ALTER SESSION SET NLS_DATE_FORMAT      = \"YYYYMMDD\"'; ";
                           sqlCreate += "     P_ALTER2 := 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = \"YYYYMMDDHH24MISSFF2\"'; ";
                           sqlCreate += "     EXECUTE IMMEDIATE P_ALTER1; ";
                           sqlCreate += "     EXECUTE IMMEDIATE P_ALTER2; ";
                           sqlCreate += "     SELECT NVL(MAX(COMMENTS), ' ') || ' ��ġ ���α׷�' INTO P_PMP_BAT_PGM_NM FROM(SELECT COMMENTS FROM ALL_TAB_COMMENTS ";
                           sqlCreate += "     WHERE OWNER = 'PMPOWN' AND TABLE_NAME = '" + oriTbName + "' UNION ALL SELECT '" + args[1] + "' FROM DUAL) T10; ";
                           sqlCreate += "     PMPOWN.SP_LOG_MRT('" + args[1] + "',P_PMP_BAT_PGM_NM,'" + args[0]
                               + "','002','EXC_���̺� ���� ����',P_RM_NI_SR_DT,P_RM_SR_DT,"
                               + "0,'ORA-20001','EXC CREATE ERROR','ERR','Y',P_RTN); END;";

                           step.executeUpdate(sqlCreate);

                           createFailCnt++;

                       }
                       System.exit(1);
                     } else {
                       if(LOGGER.isInfoEnabled()) {
                           LOGGER.info("    ��" + excCreDate + excCreTime + " : EXC ���̺� ���� ����!");
                           totLog += "    ��" + excCreDate + excCreTime + " : EXC ���̺� ���� ����!\n";
                           //LOGGER.info("  =============================================================");

                           sqlCreate += " DECLARE P_RTN           VARCHAR2(10)  :='';";
                           sqlCreate += " P_RM_NI_SR_DT           VARCHAR2(16)  := '" + stDate + stTime + "00';";
                           sqlCreate += " P_RM_SR_DT              VARCHAR2(16)  := '" + stDate + stTime + "00';";
                           sqlCreate += " P_ALTER1                VARCHAR2(1000);";
                           sqlCreate += " P_ALTER2                VARCHAR2(1000);";
                           sqlCreate += " P_PMP_BAT_PGM_NM        VARCHAR2(100) := '';";
                           sqlCreate += " BEGIN ";
                           sqlCreate += "     P_ALTER1 := 'ALTER SESSION SET NLS_DATE_FORMAT      = \"YYYYMMDD\"'; ";
                           sqlCreate += "     P_ALTER2 := 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = \"YYYYMMDDHH24MISSFF2\"'; ";
                           sqlCreate += "     EXECUTE IMMEDIATE P_ALTER1; ";
                           sqlCreate += "     EXECUTE IMMEDIATE P_ALTER2; ";
                           sqlCreate += "     SELECT NVL(MAX(COMMENTS), ' ') || ' ��ġ ���α׷�' INTO P_PMP_BAT_PGM_NM FROM(SELECT COMMENTS FROM ALL_TAB_COMMENTS ";
                           sqlCreate += "     WHERE OWNER = 'PMPOWN' AND TABLE_NAME = '" + oriTbName + "' UNION ALL SELECT '" + args[1] + "' FROM DUAL) T10; ";
                           sqlCreate += "     PMPOWN.SP_LOG_MRT('" + args[1] + "',P_PMP_BAT_PGM_NM,'" + args[0]
                               + "','001','EXC_���̺� ���� ����',P_RM_NI_SR_DT,P_RM_SR_DT,"
                               + "0,' ','','S','Y',P_RTN); END;";

                           step.executeUpdate(sqlCreate);
                       }
                     }
                   }
               } catch(Exception chke) {
                 if(LOGGER.isErrorEnabled()) LOGGER.error("  " + chke.getMessage());
               } finally {
                 if(psCheck !=null) {try {psCheck.close();} catch(Exception conne) {}}
                 if(conCheck !=null) {try {conCheck.close();} catch(Exception conne) {}}
               }

               /*------------------------------------------------------------------------------*/
               /* EXCHANG TABLE PMPOWN.WCPCM003TM TRUNCATE ���⺻ CHECK                      */
               /*------------------------------------------------------------------------------*/

               Connection conCheck1_1 = null;
               PreparedStatement psCheck1_1 = null;
               ResultSet resCheck1_1 = null;

               try {

                   conCheck1_1 = ods.getConnection();

                   String ynSql = "SELECT /*+ FULL(T10) PARALLEL(4) */ COUNT(1) AS CNT FROM PMPOWN.WCPCM003TM T10 WHERE TBL_NM = 'EXC_" + oriTbName + "'";
                   psCheck1_1 = conCheck1_1.prepareStatement(ynSql);
                   resCheck1_1 = psCheck1_1.executeQuery();
                   
                   calendar = Calendar.getInstance();
                   df = new SimpleDateFormat("yyyyMMdd");
                   chkDate = df.format(calendar.getTime());
                   df = new SimpleDateFormat("HHmmss");
                   chkTime = df.format(calendar.getTime());

                   if(resCheck1_1.next()) {
                     int ckCnt = resCheck1_1.getInt("CNT");

                     if (ckCnt > 0) {
                       if(LOGGER.isErrorEnabled()) {
                           LOGGER.info("    ��" + chkDate + chkTime + " : TRUNCATE ���⺻ ����!");
                           totLog += "    ��" + chkDate + chkTime + " : TRUNCATE ���⺻ ����!\n";
                       }
                     } else {
                       if(LOGGER.isErrorEnabled()) {
                           LOGGER.info("    ��" + chkDate + chkTime + " : TRUNCATE ���⺻ ������!");
                           totLog += "    ��" + chkDate + chkTime + " : TRUNCATE ���⺻ ������!\n";
                           defaultCnt++;
                       }
                     //System.exit(1);
                     }
                   }

               } catch(Exception chke) {
                 if(LOGGER.isErrorEnabled()) LOGGER.error("  " + chke.getMessage());
               } finally {
                 if(psCheck1_1 !=null) {try {psCheck1_1.close();} catch(Exception conne) {}}
                 if(conCheck1_1 !=null) {try {conCheck1_1.close();} catch(Exception conne) {}}
               }

               /*------------------------------------------------------------------------------*/
               /* Control File ����                                                            */
               /*------------------------------------------------------------------------------*/
               Connection conCheck000 = null;
               PreparedStatement psCheck000 = null;
               ResultSet resCheck000 = null;

               conPath = conDir + "/" + args[1].substring(4, 9) + "/" + oriTbName + "_" + args[0] + ".ctl";

               BufferedWriter conout = new BufferedWriter(new FileWriter(conPath));

               try {

                 conCheck000 = ods.getConnection();

                 String colSql  = "SELECT\n";
                        colSql += "       A.OWNER\n";
                        colSql += "     , A.TABLE_NAME\n";
                        colSql += "     , B.COMMENTS       AS TABLE_COMMENTS\n";
                        colSql += "     , A.COLUMN_NAME\n";
                        colSql += "     , nvl(C.COMMENTS,'����')       AS COLUMN_COMMENTS\n";
                        colSql += "     , A.DATA_TYPE\n";
                        colSql += "     , A.DATA_LENGTH\n";
                        colSql += "     , A.DATA_PRECISION\n";
                        colSql += "     , nvl(A.DATA_SCALE,'-999')\n    AS DATASCALE";
                        colSql += "     , A.NULLABLE\n";
                        colSql += "     , A.COLUMN_ID\n";
                        colSql += "     , A.DEFAULT_LENGTH\n";
                        colSql += "     , TRIM(REPLACE(DBMS_XMLGEN.GETXMLTYPE('SELECT DATA_DEFAULT\n";
                        colSql += "                                              FROM ALL_TAB_COLS\n";
                        colSql += "                                             WHERE OWNER = '''||A.OWNER||'''\n";
                        colSql += "                                               AND TABLE_NAME = '''||A.TABLE_NAME||'''\n";
                        colSql += "                                               AND COLUMN_NAME = '''||A.COLUMN_NAME||''' ').EXTRACT('//text()'),'&apos,','''')) AS DATA_DEFAULT\n";
                        colSql += "     , (CASE WHEN D.COLUMN_NAME IS NULL THEN 'N' ELSE 'Y' END) AS PK_YN\n";
                        colSql += "  FROM ALL_TAB_COLS A\n";
                        colSql += "  LEFT\n";
                        colSql += "  JOIN ALL_TAB_COMMENTS B\n";
                        colSql += "    ON B.OWNER = A.OWNER\n";
                        colSql += "   AND B.TABLE_NAME = A.TABLE_NAME\n";
                        colSql += "  LEFT\n";
                        colSql += "  JOIN ALL_COL_COMMENTS C\n";
                        colSql += "    ON C.OWNER = A.OWNER\n";
                        colSql += "   AND C.TABLE_NAME = A.TABLE_NAME\n";
                        colSql += "   AND C.COLUMN_NAME = A.COLUMN_NAME\n";
                        colSql += "  LEFT\n";
                        colSql += "  JOIN ALL_IND_COLUMNS D\n";
                        colSql += "    ON D.TABLE_OWNER = A.OWNER\n";
                        colSql += "   AND D.TABLE_NAME = A.TABLE_NAME\n";
                        colSql += "   AND D.COLUMN_NAME = A.COLUMN_NAME\n";
                        colSql += "   AND (D.INDEX_NAME LIKE 'PK%' OR D.INDEX_NAME LIKE '%PK')\n";
                        colSql += " WHERE A.OWNER = 'PMPOWN'\n";
                        colSql += "   AND A.TABLE_NAME = 'EXC_" + oriTbName + "'\n";
                        colSql += "   AND A.COLUMN_NAME NOT LIKE '%PSWD'\n";
                        colSql += "   AND A.HIDDEN_COLUMN = 'NO'\n";
                        colSql += " ORDER BY A.OWNER\n";
                        colSql += "        , A.TABLE_NAME\n";
                        colSql += "        , A.COLUMN_ID\n";

                 psCheck000 = conCheck000.prepareStatement(colSql);
                 resCheck000 = psCheck000.executeQuery();

                 String colList = "";

                 while(resCheck000.next()) {
                    String owner         = resCheck000.getString("OWNER");
                    String table         = resCheck000.getString("TABLE_NAME");
                    String tableNm       = resCheck000.getString("TABLE_COMMENTS");
                    String column        = resCheck000.getString("COLUMN_NAME");
                    String columnNm      = resCheck000.getString("COLUMN_COMMENTS");
                    String dataType      = resCheck000.getString("DATA_TYPE");
                    String dataLength    = resCheck000.getString("DATA_LENGTH");
                    String dataPrecision = resCheck000.getString("DATA_PRECISION");
                    String dataScale     = resCheck000.getString("DATASCALE");
                    String nulAble       = resCheck000.getString("NULLABLE");
                    int colId            = resCheck000.getInt("COLUMN_ID");
                    String defLength     = resCheck000.getString("DEFAULT_LENGTH");
                    String dataDef       = resCheck000.getString("DATA_DEFAULT");
                    String pkYn          = resCheck000.getString("PK_YN");

                    String columnList = null;

                    if(colId == 1) {
                       if(dataType.equals("VARCHAR2")) {
                         columnList = "   " + column + " CHAR\n";
                       } else if (!dataScale.equals(-999) && dataType.equals("NUMBER")) {
                         columnList = "   " + column + " DECIMAL EXTERNAL\n";
                       } else if (dataType.equals("NUMBER") || dataType.equals("INTEGER")) {
                         columnList = "   " + column + " INTEGER EXTERNAL\n";
                       } else {
                         columnList = "   " + column + " " + dataType + "\n";
                       }
                    } else {
                       if (oriTbName.equals("WCPMI001T1") && column.equals("VST_UUID")) {
                         columnList = " , " + column + " CHAR \"nvl(:" + column + ",' ')\"\n";
                       } else if (oriTbName.equals("WCPMI001T1") && column.equals("PAGE_PROC_CD")) {
                         columnList = " , " + column + " CHAR \"nvl(:" + column + ",'0')\"\n";
                       //} else if (oriTbName.equals("WCPMI001T1") && column.equals("MKT_LOAD_PGM_ID")) {
                       //  columnList = " , " + column + " \"'PMPEtlrun'\"\n";
                       //} else if (oriTbName.equals("WCPMI001T1") && column.equals("MKT_LOAD_DH")) {
                       //  columnList = " , " + column + " \"'" + stDate + stTime + "'\"\n";
                       } else if(oriTbName.equals("WCPMI001T1") && (column.equals("TRIG_SUB_INFO") || column.equals("MENU_TITLE") || column.equals("URL_INFO") || column.equals("REF_URL_INFO") || column.equals("BRW_INFO") || column.equals("CMP_ID"))) {
                         columnList = " , " + column + " CHAR (2000)\n";
                       } else if(dataType.equals("VARCHAR2")) {
                         columnList = " , " + column + " CHAR\n";
                       } else if (!dataScale.equals(-999) && dataType.equals("NUMBER")) {
                         columnList = " , " + column + " DECIMAL EXTERNAL\n";
                       } else if (dataType.equals("NUMBER") || dataType.equals("INTEGER")) {
                         columnList = " , " + column + " INTEGER EXTERNAL\n";
                       } else {
                         columnList = " , " + column + " " + dataType + "\n";
                       }
                    }

                    colList += columnList;
                 }

                 conFile = "OPTIONS(ERRORS=0)\n";
                 conFile  += "LOAD DATA\n";
                 if (oriTbName.equals("WCPMI001T1")) {
                   conFile += "CHARACTERSET KO16MSWIN949\n";
                   conFile += "INFILE '" + samDir + "/" + oriTbName + "_" + args[0] + ".dat' \"STR '|^,|\\n'\"\n";
                 } else {
                   conFile += "CHARACTERSET KO16MSWIN949\n";
                   conFile += "INFILE '" + samDir + "/" + oriTbName + "_" + args[0] + ".dat' \"STR '|^,|\\n'\"\n";
                 }

                 conFile += "APPEND\n";
                 conFile += "INTO TABLE PMPOWN.EXC_" + oriTbName + "\n";
                 if (oriTbName.equals("WCPMI001T1")) {
                   conFile += "FIELDS TERMINATED BY '|^,|'\n";
                 } else {
                   conFile += "FIELDS TERMINATED BY '|^,|'\n";
                 }

                 conFile += "TRAILING NULLCOLS\n";
                 conFile += "(\n";
                 conFile += colList;
                 conFile += ")\n";

                 conout.write(conFile);
                 conout.newLine();
                 conout.close();

                 calendar = Calendar.getInstance();
                 df = new SimpleDateFormat("yyyyMMdd");
                 conCreDate = df.format(calendar.getTime());
                 df = new SimpleDateFormat("HHmmss");
                 conCreTime = df.format(calendar.getTime());
                   
                 File f = new File(conPath);

                 if(f.exists()) {
                   LOGGER.info("    ��" + conCreDate + conCreTime + " : CONTROL ���� ���� ����!");
                 } else {
                   LOGGER.info("    ��" + conCreDate + conCreTime + " : CONTROL ���� ���� ����!");
                 }

               } catch(Exception ce) {
                 if(LOGGER.isErrorEnabled()) LOGGER.error("  " + ce.getMessage());
               } finally {
                 if(psCheck000 !=null) {try {psCheck000.close();} catch(Exception conne) {}}
                 if(conCheck000 !=null) {try {conCheck000.close();} catch(Exception conne) {}}
                 conout.close();
               }

               /*------------------------------------------------------------------------------*/
               /* EXCHANG TABLE TRUNCATE                                                       */
               /*------------------------------------------------------------------------------*/

               if(createFailCnt == 1) {
                 LOGGER.info("    ��EXC TABLE NOT EXIST!");
                 totLog += "    ��EXC TABLE NOT EXIST!\n";
               } else if (defaultCnt == 1 ) {
                 LOGGER.info("    ��TRUNCATE FAIL");
                 totLog += "    ��TRUNCATE FAIL\n";
               } else {
               String sqlExtr = " DECLARE P_RTN     VARCHAR2(10) := ''; ";
               sqlExtr += " BEGIN ";
               sqlExtr += " PMPOWN.SP_PMPOWN_TRUNCATE(P_RTN, 'PMPOWN', 'EXC_" + oriTbName + "', ''); END;";

               step.executeUpdate(sqlExtr);
               }

               Connection conCheck1 = null;
               PreparedStatement psCheck1 = null;
               ResultSet resCheck1 = null;

               try {

                   conCheck1 = ods.getConnection();

                   String trynSql = "SELECT /*+ FULL(T10) PARALLEL(4) */ COUNT(1) AS CNT FROM PMPOWN.EXC_" + oriTbName + " T10";
                   psCheck1 = conCheck1.prepareStatement(trynSql);
                   resCheck1 = psCheck1.executeQuery();
                   
                   calendar = Calendar.getInstance();
                   df = new SimpleDateFormat("yyyyMMdd");
                   excTruncateDate = df.format(calendar.getTime());
                   df = new SimpleDateFormat("HHmmss");
                   excTruncateTime = df.format(calendar.getTime());

                   if(resCheck1.next()) {
                     int checkCnt = resCheck1.getInt("CNT");
                     String sqlTruncate = "";

                     /*-----------------------------------------------------------------------------------*/
                     /* EXC TABLE TRUNCATE ���� ���� �α� INSERT                                          */
                     /*-----------------------------------------------------------------------------------*/

                     if (checkCnt > 0) {
                       if(LOGGER.isErrorEnabled()) {
                           LOGGER.info("    ��" + excTruncateDate + excTruncateTime + " : EXC ���̺� TRUNCATE ����!");
                           totLog += "    ��" + excTruncateDate + excTruncateTime + " : EXC ���̺� TRUNCATE ����!\n";

                           sqlTruncate += " DECLARE P_RTN           VARCHAR2(10)  :='';";
                           sqlTruncate += " P_RM_NI_SR_DT           VARCHAR2(16)  := '" + stDate + stTime + "00';";
                           sqlTruncate += " P_RM_SR_DT              VARCHAR2(16)  := '" + stDate + stTime + "00';";
                           sqlTruncate += " P_ALTER1                VARCHAR2(1000);";
                           sqlTruncate += " P_ALTER2                VARCHAR2(1000);";
                           sqlTruncate += " P_PMP_BAT_PGM_NM        VARCHAR2(100) := '';";
                           sqlTruncate += " BEGIN ";
                           sqlTruncate += "     P_ALTER1 := 'ALTER SESSION SET NLS_DATE_FORMAT      = \"YYYYMMDD\"'; ";
                           sqlTruncate += "     P_ALTER2 := 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = \"YYYYMMDDHH24MISSFF2\"'; ";
                           sqlTruncate += "     EXECUTE IMMEDIATE P_ALTER1; ";
                           sqlTruncate += "     EXECUTE IMMEDIATE P_ALTER2; ";
                           sqlTruncate += "     SELECT NVL(MAX(COMMENTS), ' ') || ' ��ġ ���α׷�' INTO P_PMP_BAT_PGM_NM FROM(SELECT COMMENTS FROM ALL_TAB_COMMENTS ";
                           sqlTruncate += "     WHERE OWNER = 'PMPOWN' AND TABLE_NAME = '" + oriTbName + "' UNION ALL SELECT '" + args[1] + "' FROM DUAL) T10; ";
                           sqlTruncate += "     PMPOWN.SP_LOG_MRT('" + args[1] + "',P_PMP_BAT_PGM_NM,'" + args[0]
                               + "','002','EXC_���̺� TRUNCATE ����',P_RM_NI_SR_DT,P_RM_SR_DT,"
                               + "0,'ORA-20002','EXC TRUNCATE ERROR','ERR','Y',P_RTN); END;";

                           step.executeUpdate(sqlTruncate);
                       }
                       System.exit(1);
                     }  else {
                       if(LOGGER.isErrorEnabled()) {
                           LOGGER.info("    ��" + excTruncateDate + excTruncateTime + " : EXC ���̺� TRUNCATE ����!");
                           totLog += "    ��" + excTruncateDate + excTruncateTime + " : EXC ���̺� TRUNCATE ����!\n";
                           //LOGGER.info("  =============================================================");

                           sqlTruncate += " DECLARE P_RTN           VARCHAR2(10)  :='';";
                           sqlTruncate += " P_RM_NI_SR_DT           VARCHAR2(16)  := '" + stDate + stTime + "00';";
                           sqlTruncate += " P_RM_SR_DT              VARCHAR2(16)  := '" + stDate + stTime + "00';";
                           sqlTruncate += " P_ALTER1                VARCHAR2(1000);";
                           sqlTruncate += " P_ALTER2                VARCHAR2(1000);";
                           sqlTruncate += " P_PMP_BAT_PGM_NM        VARCHAR2(100) := '';";
                           sqlTruncate += " BEGIN ";
                           sqlTruncate += "     P_ALTER1 := 'ALTER SESSION SET NLS_DATE_FORMAT      = \"YYYYMMDD\"'; ";
                           sqlTruncate += "     P_ALTER2 := 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = \"YYYYMMDDHH24MISSFF2\"'; ";
                           sqlTruncate += "     EXECUTE IMMEDIATE P_ALTER1; ";
                           sqlTruncate += "     EXECUTE IMMEDIATE P_ALTER2; ";
                           sqlTruncate += "     SELECT NVL(MAX(COMMENTS), ' ') || ' ��ġ ���α׷�' INTO P_PMP_BAT_PGM_NM FROM(SELECT COMMENTS FROM ALL_TAB_COMMENTS ";
                           sqlTruncate += "     WHERE OWNER = 'PMPOWN' AND TABLE_NAME = '" + oriTbName + "' UNION ALL SELECT '" + args[1] + "' FROM DUAL) T10; ";
                           sqlTruncate += "     PMPOWN.SP_LOG_MRT('" + args[1] + "',P_PMP_BAT_PGM_NM,'" + args[0]
                               + "','002','EXC_���̺� TRUNCATE ����',P_RM_NI_SR_DT,P_RM_SR_DT,"
                               + "0,' ','','S','Y',P_RTN); END;";

                           step.executeUpdate(sqlTruncate);
                       }
                     }
                   }
               } catch(Exception chke) {
                 if(LOGGER.isErrorEnabled()) LOGGER.error("  " + chke.getMessage());
               } finally {
                 if(psCheck1 !=null) {try {psCheck1.close();} catch(Exception conne) {}}
                 if(conCheck1 !=null) {try {conCheck1.close();} catch(Exception conne) {}}
               }

               /*------------------------------------------------------------------------------*/
               /* SQLLOADER SAM���� EXEC ���̺� ����                                           */
               /*------------------------------------------------------------------------------*/
               
               if(oriTbName.equals("WCPMI001T1")){
                  sqlLoaderBadChkCnt = 999999;
               } else {
                  sqlLoaderBadChkCnt = 0;
               }
               String sqlloader  = "sqlldr userid=" + pmpUser + "@" + dbService + "/" + pmpPass + " control=" + conPath;
                      sqlloader += " data=" + samDir + "/" + oriTbName + "_" + args[0] + ".dat";
                      sqlloader += " log=" + loadLogPath;
                      sqlloader += " bad=" + badDir + "/" + oriTbName + "_" + args[0] + ".bad direct=y errors=" + sqlLoaderBadChkCnt;

               Process p = Runtime.getRuntime().exec(sqlloader);
               BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
                   
               try {

                  String line = null;

                  while ((line = br.readLine()) != null) {
                       //LOGGER.info(line);
                  }

                  p.waitFor();
                  
                  calendar = Calendar.getInstance();
                  df = new SimpleDateFormat("yyyyMMdd");
                  sqlLoaderDate = df.format(calendar.getTime());
                  df = new SimpleDateFormat("HHmmss");
                  sqlLoaderTime = df.format(calendar.getTime());
               
                  LOGGER.info("    ��" + sqlLoaderDate + sqlLoaderTime + " : SQLLOADER �۾� ����");
               } catch(Exception chke) {
                 if(LOGGER.isErrorEnabled()) LOGGER.error("  " + chke.getMessage());
               } finally {
                 br.close();
               }

               /*------------------------------------------------------------------------------*/
               /* EXCHANGE TABLE REBUILD                                                       */
               /*------------------------------------------------------------------------------*/
               Connection conn1 = ods.getConnection();
               CallableStatement cstmt = null;
               String sqlRebuild;
               oriTbName = args[1].substring(4, args[1].length() - 3);
               try {
                    sqlRebuild = "{call PMPOWN.SP_INDEX_REBUILD(?,?,?)}";

                    cstmt = conn1.prepareCall(sqlRebuild);

                    cstmt.setString(1," ");
                    cstmt.setString(2,"PMPOWN");
                    cstmt.setString(3,"EXC_" + oriTbName);

                    cstmt.execute();

               } catch(Exception e) {
                 if(LOGGER.isErrorEnabled()) LOGGER.error("  " + e.getMessage());
                 System.exit(1);
               } finally {
                 if(cstmt !=null) {try {cstmt.close();} catch(Exception conne) {}}
                 if(conn1 !=null) {try {conn1.close();} catch(Exception conne) {}}
               }


               /*------------------------------------------------------------------------------*/
               /* EXC ���̺� �Ǽ� üũ                                                         */
               /*------------------------------------------------------------------------------*/

               Connection conCheckCnt = null;
               PreparedStatement psCheckCnt = null;
               ResultSet resCheckCnt = null;

               try {

                   conCheckCnt = ods.getConnection();

                   String cntSql = "SELECT /*+ FULL(T10) PARALLEL(4) */ COUNT(1) AS CNT FROM PMPOWN.EXC_" + oriTbName;
                   psCheckCnt = conCheckCnt.prepareStatement(cntSql);
                   resCheckCnt = psCheckCnt.executeQuery();

                   calendar = Calendar.getInstance();
                   df = new SimpleDateFormat("yyyyMMdd");
                   excCntDate = df.format(calendar.getTime());
                   df = new SimpleDateFormat("HHmmss");
                   excCntTime = df.format(calendar.getTime());
               
                   if(resCheckCnt.next()) {
                     int excCnt = resCheckCnt.getInt("CNT");
                     excCount = excCnt;
                   }
                   
                   if(excCount == 0) {
                     LOGGER.info("    ��" + excCntDate + excCntTime + " : EXC ���̺� �Ǽ� 0��! �۾� ����");
                     System.exit(1);
                   }

               } catch(Exception chke) {
                 if(LOGGER.isErrorEnabled()) LOGGER.error("  " + chke.getMessage());
               } finally {
                 if(psCheckCnt !=null) {try {psCheckCnt.close();} catch(Exception conne) {}}
                 if(conCheckCnt !=null) {try {conCheckCnt.close();} catch(Exception conne) {}}
               }


               /*------------------------------------------------------------------------------*/
               /* EXCHANGE ����                                                                */
               /*------------------------------------------------------------------------------*/

               Connection conCheck2 = null;
               PreparedStatement psCheck2 = null;
               int resCheck2 = 0;

               String sqlExchange = "";
               try {

                   if (defaultCnt == 1) {
                      LOGGER.info("    ��EXEC TABLE TRUNCATE NOT PERFORMED");
                      totLog += "    ��EXEC ���̺� TRUNCATE NOT PERFORMED\n";
                   } else {
                     Connection conCheck2_1 = null;
                     PreparedStatement psCheck2_1 = null;
                     ResultSet resCheck2_1 = null;

                     /*------------------------------------------------------------------------------*/
                     /* ��Ƽ�Ǹ� üũ ����                                                           */
                     /*------------------------------------------------------------------------------*/
                     try {

                       conCheck2_1 = ods.getConnection();

                       String partitionSql = "SELECT PARTITION_NAME FROM ALL_TAB_PARTITIONS WHERE TABLE_OWNER = 'PMPOWN' AND TABLE_NAME = '" + oriTbName + "' AND ROWNUM = 1";
                       psCheck2_1 = conCheck2_1.prepareStatement(partitionSql);
                       resCheck2_1 = psCheck2_1.executeQuery();

                       if(resCheck2_1.next()) {
                         String checkName = resCheck2_1.getString("PARTITION_NAME");
                         if (checkName.substring(14).length() == 2) {
                           String exTable = " DECLARE P_RTN     VARCHAR2(10) := ''; ";
                                 exTable += " BEGIN ";
                                 exTable += " PMPOWN.SP_EXCHANGE_PTS(P_RTN, 'PMPOWN', 'EXC_" + oriTbName + "', '" + oriTbName + "', '" + oriTbName + "_PTR" + checkName.substring(14) + "'); END;";
                           resCheck2 = step.executeUpdate(exTable);
                         } else if (checkName.substring(14).length() == 6) {
                           String exTable = " DECLARE P_RTN     VARCHAR2(10) := ''; ";
                                 exTable += " BEGIN ";
                                 exTable += " PMPOWN.SP_EXCHANGE_PTS(P_RTN, 'PMPOWN', 'EXC_" + oriTbName + "', '" + oriTbName + "', '" + oriTbName + "_PTR" + args[0].substring(0,6) + "'); END;";
                           resCheck2 = step.executeUpdate(exTable);
                         } else if (checkName.substring(14).length() == 8) {
                           String exTable = " DECLARE P_RTN     VARCHAR2(10) := ''; ";
                                 exTable += " BEGIN ";
                                 exTable += " PMPOWN.SP_EXCHANGE_PTS(P_RTN, 'PMPOWN', 'EXC_" + oriTbName + "', '" + oriTbName + "', '" + oriTbName + "_PTR" + args[0] + "'); END;";
                           resCheck2 = step.executeUpdate(exTable);
                         }

                       }
                     } catch (Exception chk) {
                         if(LOGGER.isErrorEnabled()) LOGGER.error("  " + chk.getMessage());
                     } finally {
                         if(psCheck2_1 !=null) {try {psCheck2_1.close();} catch(Exception conne) {}}
                         if(conCheck2_1 !=null) {try {conCheck2_1.close();} catch(Exception conne) {}}
                     }


                   }
                   
               /*------------------------------------------------------------------------------*/
               /* EXCHANGE SUCCESS Y/N -- WCPCM002TH �α� Ȯ��                                 */
               /*------------------------------------------------------------------------------*/

               Connection conCheck3 = null;
               PreparedStatement psCheck3 = null;
               ResultSet resCheck3 = null;

               try {

                   conCheck3 = ods.getConnection();

                   String excLogSql = "SELECT /*+ FULL(T10) PARALLEL(4) */ TGT_TBL_NM, SUBSTR(MAX(BAT_WK_STA_DTL_DH || BAT_ERR_NO),17) AS ERR_NO FROM WCPCM002TH WHERE TGT_TBL_NM = '" + oriTbName + "' GROUP BY TGT_TBL_NM";
                   psCheck3 = conCheck3.prepareStatement(excLogSql);
                   resCheck3 = psCheck3.executeQuery();

                   calendar = Calendar.getInstance();
                   df = new SimpleDateFormat("yyyyMMdd");
                   excParDate = df.format(calendar.getTime());
                   df = new SimpleDateFormat("HHmmss");
                   excParTime = df.format(calendar.getTime());
                   
                   if(resCheck3.next()) {
                     int errCode = resCheck3.getInt("ERR_NO");

                     if (errCode == 0) {
                       if(LOGGER.isErrorEnabled()) {
                           LOGGER.info("    ��" + excParDate + excParTime + " : EXCHANGE PARTITIONS ����!");
                           totLog += "    ��" + excParDate + excParTime + " : EXCHANGE PARTITIONS ����!\n";
                       }
                     } else {
                       if(LOGGER.isErrorEnabled()) {
                           LOGGER.info("    ��" + excParDate + excParTime + " : EXCHANGE PARTITIONS ERROR!");
                           totLog += "    ��" + excParDate + excParTime + " : EXCHANGE PARTITIONS ERROR!\n";
                           excPartition++;
                       }
                     System.exit(1);
                     }
                   }

               } catch(Exception chke) {
                 if(LOGGER.isErrorEnabled()) LOGGER.error("  " + chke.getMessage());
               } finally {
                 if(psCheck3 !=null) {try {psCheck3.close();} catch(Exception conne) {}}
                 if(conCheck3 !=null) {try {conCheck3.close();} catch(Exception conne) {}}
               }


                   /*-----------------------------------------------------------------------------------*/
                   /* EXCHANGE TABLE SUCCESS Y/N                                                        */
                   /*-----------------------------------------------------------------------------------*/

                   if (resCheck2 == 1 && createFailCnt == 0 && defaultCnt == 0 && excPartition == 0) {
                     if(LOGGER.isErrorEnabled()) {
                        LOGGER.info("    ��" + excParDate + excParTime + " : EXCHANGE SUCCESS!");
                        totLog += "    ��" + excParDate + excParTime + " : EXCHANGE SUCCESS!\n";
                        LOGGER.info("  =============================================================");

                        sqlExchange += " DECLARE P_RTN           VARCHAR2(10)  :='';";
                        sqlExchange += " P_RM_NI_SR_DT           VARCHAR2(16)  := '" + stDate + stTime + "00';";
                        sqlExchange += " P_RM_SR_DT              VARCHAR2(16)  := '" + stDate + stTime + "00';";
                        sqlExchange += " P_ALTER1                VARCHAR2(1000);";
                        sqlExchange += " P_ALTER2                VARCHAR2(1000);";
                        sqlExchange += " P_PMP_BAT_PGM_NM        VARCHAR2(100) := '';";
                        sqlExchange += " BEGIN ";
                        sqlExchange += "     P_ALTER1 := 'ALTER SESSION SET NLS_DATE_FORMAT      = \"YYYYMMDD\"'; ";
                        sqlExchange += "     P_ALTER2 := 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = \"YYYYMMDDHH24MISSFF2\"'; ";
                        sqlExchange += "     EXECUTE IMMEDIATE P_ALTER1; ";
                        sqlExchange += "     EXECUTE IMMEDIATE P_ALTER2; ";
                        sqlExchange += "     SELECT NVL(MAX(COMMENTS), ' ') || ' ��ġ ���α׷�' INTO P_PMP_BAT_PGM_NM FROM(SELECT COMMENTS FROM ALL_TAB_COMMENTS ";
                        sqlExchange += "     WHERE OWNER = 'PMPOWN' AND TABLE_NAME = '" + oriTbName + "' UNION ALL SELECT '" + args[1] + "' FROM DUAL) T10; ";
                        sqlExchange += "     PMPOWN.SP_LOG_MRT('" + args[1] + "',P_PMP_BAT_PGM_NM,'" + args[0]
                            + "','003','EXCHANGE ����',P_RM_NI_SR_DT,P_RM_SR_DT,"
                            + excCount + ",' ','','S','Y',P_RTN); END;";

                        step.executeUpdate(sqlExchange);
                     }
                   } else if (resCheck2 == 1 && createFailCnt == 1) {
                     if(LOGGER.isErrorEnabled()) {
                        LOGGER.info("    ��" + excParDate + excParTime + " : EXCHANGE ���̺� ������");
                        totLog += "    ��" + excParDate + excParTime + " : EXCHANGE ���̺� ������\n";

                        sqlExchange += " DECLARE P_RTN           VARCHAR2(10)  :='';";
                        sqlExchange += " P_RM_NI_SR_DT           VARCHAR2(16)  := '" + stDate + stTime + "00';";
                        sqlExchange += " P_RM_SR_DT              VARCHAR2(16)  := '" + stDate + stTime + "00';";
                        sqlExchange += " P_ALTER1                VARCHAR2(1000);";
                        sqlExchange += " P_ALTER2                VARCHAR2(1000);";
                        sqlExchange += " P_PMP_BAT_PGM_NM        VARCHAR2(100) := '';";
                        sqlExchange += " BEGIN ";
                        sqlExchange += "     P_ALTER1 := 'ALTER SESSION SET NLS_DATE_FORMAT      = \"YYYYMMDD\"'; ";
                        sqlExchange += "     P_ALTER2 := 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = \"YYYYMMDDHH24MISSFF2\"'; ";
                        sqlExchange += "     EXECUTE IMMEDIATE P_ALTER1; ";
                        sqlExchange += "     EXECUTE IMMEDIATE P_ALTER2; ";
                        sqlExchange += "     SELECT NVL(MAX(COMMENTS), ' ') || ' ��ġ ���α׷�' INTO P_PMP_BAT_PGM_NM FROM(SELECT COMMENTS FROM ALL_TAB_COMMENTS ";
                        sqlExchange += "     WHERE OWNER = 'PMPOWN' AND TABLE_NAME = '" + oriTbName + "' UNION ALL SELECT '" + args[1] + "' FROM DUAL) T10; ";
                        sqlExchange += "     PMPOWN.SP_LOG_MRT('" + args[1] + "',P_PMP_BAT_PGM_NM,'" + args[0]
                            + "','003','EXCHANGE ���̺� ������',P_RM_NI_SR_DT,P_RM_SR_DT,"
                            + "0,'ORA-20003','EXCHANGE ERROR','ERR','Y',P_RTN); END;";

                        step.executeUpdate(sqlExchange);
                     }
                     System.exit(1);
                   }
               } catch(Exception chke) {
                 if(LOGGER.isErrorEnabled()) {
                   if(LOGGER.isErrorEnabled()) 
                   LOGGER.error("    ��" + excParDate + excParTime + " : EXCHANGE FAIL\n" + chke.getMessage());
                   totLog += "    ��" + excParDate + excParTime + " : EXCHANGE FAIL\n";
//                   LOGGER.info("    003... EXCHANGE ����\n" + chke.getMessage());

                   sqlExchange += " DECLARE P_RTN           VARCHAR2(10)  :='';";
                   sqlExchange += " P_RM_NI_SR_DT           VARCHAR2(16)  := '" + stDate + stTime + "00';";
                   sqlExchange += " P_RM_SR_DT              VARCHAR2(16)  := '" + stDate + stTime + "00';";
                   sqlExchange += " P_ALTER1                VARCHAR2(1000);";
                   sqlExchange += " P_ALTER2                VARCHAR2(1000);";
                   sqlExchange += " P_PMP_BAT_PGM_NM        VARCHAR2(100) := '';";
                   sqlExchange += " BEGIN ";
                   sqlExchange += "     P_ALTER1 := 'ALTER SESSION SET NLS_DATE_FORMAT      = \"YYYYMMDD\"'; ";
                   sqlExchange += "     P_ALTER2 := 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = \"YYYYMMDDHH24MISSFF2\"'; ";
                   sqlExchange += "     EXECUTE IMMEDIATE P_ALTER1; ";
                   sqlExchange += "     EXECUTE IMMEDIATE P_ALTER2; ";
                   sqlExchange += "     SELECT NVL(MAX(COMMENTS), ' ') || ' ��ġ ���α׷�' INTO P_PMP_BAT_PGM_NM FROM(SELECT COMMENTS FROM ALL_TAB_COMMENTS ";
                   sqlExchange += "     WHERE OWNER = 'PMPOWN' AND TABLE_NAME = '" + oriTbName + "' UNION ALL SELECT '" + args[1] + "' FROM DUAL) T10; ";
                   sqlExchange += "     PMPOWN.SP_LOG_MRT('" + args[1] + "',P_PMP_BAT_PGM_NM,'" + args[0]
                       + "','003','EXCHANGE ����',P_RM_NI_SR_DT,P_RM_SR_DT,"
                       + "0,'ORA-20003','EXCHANGE ERROR','ERR','Y',P_RTN); END;";

                   step.executeUpdate(sqlExchange);
                   System.exit(1);
                 }
               } finally {
                 if(psCheck2 !=null) {try {psCheck2.close();} catch(Exception conne) {}}
                 if(conCheck2 !=null) {try {conCheck2.close();} catch(Exception conne) {}}
               }
               
               // BAD ���� ���翩�� üũ
               String cmdbadfilechk[] = {"sh", "-c", "find /pmp/dwdb/bad/" + args[1].substring(4, 14) + "_" + args[0] + ".bad -type f | wc -l"};
               Process procprcbadfilechk = rt.exec(cmdbadfilechk);
               procprcbadfilechk.waitFor();
               
               BufferedReader cbrbadfilechk = new BufferedReader(new InputStreamReader(procprcbadfilechk.getInputStream()));
               String strbadfilechk = "";
               
               try {
                   strbadfilechk = cbrbadfilechk.readLine();
                   if(strbadfilechk.equals("1") && oriTbName.equals("WCPMI001T1")) {
                      LOGGER.info("    ��ERROR      : BAD���� üũ���");
                   }else if(strbadfilechk.equals("1")) {
                     LOGGER.info("    ��ERROR      : BAD���� üũ���");
                     System.exit(1);
                   }
               
               } catch (Exception e) {
                   if(LOGGER.isErrorEnabled()) LOGGER.error("  " + e.getMessage());
               } finally {
                   cbrbadfilechk.close();
               }
             }
               calendar = Calendar.getInstance();
               df = new SimpleDateFormat("yyyyMMdd");
               edDate = df.format(calendar.getTime());
               df = new SimpleDateFormat("HHmmss");
               edTime = df.format(calendar.getTime());

               totLog += "  =============================================================" + "\n";
               totLog += "                         <���α׷� ����>" + "\n";
               totLog += "    ���������       : " + args[0] + "\n";
               totLog += "    �����α׷���     : " + args[1] + "\n";
               totLog += "    ���۾����۽ð�   : " + stDate + stTime + "\n";
               totLog += "    ���۾�����ð�   : " + edDate + edTime + "\n";
               totLog += "    ���۾��ҿ�ð�   : " + changeTime(getTime(stDate + stTime, edDate + edTime) + "") + "\n";
               totLog += "  =============================================================" + "\n";

               LOGGER.info("  =============================================================");
               LOGGER.info("                         <���α׷� ����>");
               LOGGER.info("    ���������       : " + args[0]);
               LOGGER.info("    �����α׷���     : " + args[1]);
               LOGGER.info("    ���۾����۽ð�   : " + stDate + stTime);
               LOGGER.info("    ���۾�����ð�   : " + edDate + edTime);
               LOGGER.info("    ���۾��ҿ�ð�   : " + changeTime(getTime(stDate + stTime, edDate + edTime) + ""));
               LOGGER.info("  =============================================================");

               resChk = 0;

            } catch (Exception e) {
              calendar = Calendar.getInstance();
              df = new SimpleDateFormat("yyyyMMdd");
              edDate = df.format(calendar.getTime());
              df = new SimpleDateFormat("HHmmss");
              edTime = df.format(calendar.getTime());

              errStr = new String("  " + e.getMessage());

              totLog += "  =============================================================" + "\n";
              totLog += "                         <���α׷� ����>" + "\n";
              totLog += "    ���۾�����ð� : " + edDate + edTime + "\n";
              totLog += "    ���۾��ҿ�ð� : " + changeTime(getTime(stDate + stTime, edDate + edTime) + "") + "\n";
              totLog += "  =============================================================" + "\n";
              totLog += errStr + "\n";
              totLog += "  =============================================================" + "\n";

              LOGGER.info("  =============================================================");
              LOGGER.info("                         <���α׷� ����>");
              LOGGER.info("    ���۾�����ð� : " + edDate + edTime);
              LOGGER.info("    ���۾��ҿ�ð� : " + changeTime(getTime(stDate + stTime, edDate + edTime) + ""));
              LOGGER.info("  =============================================================");
              LOGGER.info(errStr);
              LOGGER.info("  =============================================================");

              resChk = 1;
            } finally{
              //BufferedWriter out = new BufferedWriter(new FileWriter(logPath));
              BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logPath),"euc-kr"));

              out.write(totLog);
              out.newLine();
              out.close();

              try{
                if(step != null && !step.isClosed()){
                    step.close();
                }

                if(conn != null && !conn.isClosed()){
                    conn.close();
                }
              }catch(Exception e){
                System.exit(1);
              }
            }
    }

    public static long getTime(String start, String end){
        Calendar cal01 = Calendar.getInstance();
        Calendar cal02 = Calendar.getInstance();
        cal01.set(
            Integer.parseInt(start.substring(0, 4)),
            Integer.parseInt(start.substring(4, 6)),
            Integer.parseInt(start.substring(6, 8)),
            Integer.parseInt(start.substring(8, 10)),
            Integer.parseInt(start.substring(10, 12)),
            Integer.parseInt(start.substring(12, 14))
        );

        cal02.set(
            Integer.parseInt(end.substring(0, 4)),
            Integer.parseInt(end.substring(4, 6)),
            Integer.parseInt(end.substring(6, 8)),
            Integer.parseInt(end.substring(8, 10)),
            Integer.parseInt(end.substring(10, 12)),
            Integer.parseInt(end.substring(12, 14))
        );

        long time = (cal02.getTime().getTime() - cal01.getTime().getTime()) / 1000;

        return time;
    }

    public static String changeTime(String second){
        String h = "";
        String m = "";
        String s = "";
        int sec = Integer.parseInt(second);

        if(sec > 3600){
            h = sec / 3600 + "";
            sec %= 3600;
        }

        if(sec >= 60){
            m = sec / 60 + "";
            sec %= 60;
        }

        if(sec < 60){
            s = sec + "";
        }

        if("".equals(h)){
            h = "00";
        }else if(h.length() < 2){
            h = "0" + h;
        }

        if("".equals(m)){
            m = "00";
        }else if(m.length() < 2){
            m = "0" + m;
        }

        if(s.length() < 2){
            s = "0" + s;
        }

        return h + ":" + m + ":" + s;
    }

    private static Properties getEnvProperties() {
        Properties prop = new Properties();

        String sbPropertiesPath = "/app/mk_batch/COM/ENV/PMPenv.ini";

        try(InputStream input = new FileInputStream(sbPropertiesPath.toString())){
            prop.load(input);
        } catch(IOException e) {
            if(LOGGER.isErrorEnabled()) LOGGER.error("  " + e.getMessage());
        }

        return prop;
    }
}
