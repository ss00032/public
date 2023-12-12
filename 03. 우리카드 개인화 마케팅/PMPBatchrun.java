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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import oracle.jdbc.pool.OracleDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PMPBatchrun {

    public static final Logger LOGGER = LoggerFactory.getLogger(PMPBatchrun.class);

    public static void main (String args [])
        throws Exception
    {
        Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();

        Properties propEnv = getEnvProperties();

        Runtime rt = Runtime.getRuntime();

        //���۽ð� ����ð�
        String stDate = "";
        String stTime = "";
        String edDate = "";
        String edTime = "";

        //Log
        String totLog   = "";
        String logPath  = "";

        //Path
        String runDir = propEnv.getProperty("RUN_DIR"); // ������
        String comDir = propEnv.getProperty("COM_DIR"); // ������
        String logDir = propEnv.getProperty("LOG_DIR"); // �αװ��
        String genDir = propEnv.getProperty("GEN_DIR"); // SQL�������
        String orgDir = propEnv.getProperty("ORG_DIR"); // SQL�������
        String oriTbName = ""; // ���̺�ID
        String mnsbScop = ""; // ��������

        //��������
        int resChk      = 0;
        String resChkStr = "";

        //�ͼ��� ����
        String errStr   = "";

        //DB���� �� ��������
        String dbInfo = "";
        String pmpUser = "";
        String pmpPass = "";

        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        stDate = df.format(calendar.getTime());
        df = new SimpleDateFormat("HHmmss");
        stTime = df.format(calendar.getTime());

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
            LOGGER.info("    �����α׷��� : " + args[1]);
            LOGGER.info("    ����۽ð�   : " + stDate + stTime);
        }

        totLog = "  =============================================================" + "\n";
        totLog += "                         <���α׷� ����>" + "\n";
        totLog += "    �����α׷�   : " + args[1] + "\n";
        totLog += "    ����۽ð�   : " + stDate + stTime + "\n";


                
        Connection conn = null;
        Statement  step = null;
        //BufferedWriter outsql = null;
        //BufferedWriter out = "";

        try{
            if (args.length < 2) {
                if(LOGGER.isInfoEnabled()) {
                    LOGGER.info("    ERROR!!");
                    LOGGER.info("    �Է� Parameter�� ��Ȯ���� �ʽ��ϴ�.");
                    LOGGER.info("  =============================================================");
                    LOGGER.info("    TITLE) java Data Loading Program");
                    LOGGER.info("    USAGE) java PMPBatchrun DATE JOBNAME");
                    LOGGER.info("       EX) java PMPBatchrun 20210417 BCD_WCPMI201TM_TG");
                    LOGGER.info("  =============================================================");
                }
            }else{
                //���뺯�� ����?
                if(args[1].substring(0,3).equals("SP_")) {
                mnsbScop   = args[1].substring(3, 8);
                } else {
                mnsbScop   = args[1].substring(4, 9);
                }

                
                genDir += mnsbScop + "/";
                orgDir += mnsbScop + "/";
                logDir += mnsbScop + "/";
                
                if(args[1].substring(0,3).equals("SP_")) {
                oriTbName = args[1].substring(3);
                } else {
                oriTbName = args[1].substring(4, args[1].length() - 3);
                }
        
                String jobTime = new java.text.SimpleDateFormat("HHmmss").format(new java.util.Date());
                String jobDate = "'" + args[0] + "'";
                String jobName = "'" + args[1] + "'";
                String jobLogName = args[0] + jobTime + args[1];
                String fileName = jobName;
                String jobJugi = "" + args[1].substring(1, 2) + "";
                String lDriver = "ZZ";
                String inParam3 = "''";
                String inParam4 = "''";
                String inParam5 = "''";
                String inParam6 = "''";

                if (args.length == 3) {
                    inParam3 = "'" + args[2] + "'";
                } else if (args.length == 4) {
                    inParam3 = "'" + args[2] + "'";
                    inParam4 = "'" + args[3] + "'";
                } else if (args.length == 5) {
                    inParam3 = "'" + args[2] + "'";
                    inParam4 = "'" + args[3] + "'";
                    inParam5 = "'" + args[4] + "'";
                } else if (args.length == 6) {
                    inParam3 = "'" + args[2] + "'";
                    inParam4 = "'" + args[3] + "'";
                    inParam5 = "'" + args[4] + "'";
                    inParam6 = "'" + args[5] + "'";
                }

                logPath = logDir + args[0] + "_" + args[1] + ".log";

                //�Է¹��� ��¥���� validation check
                if(jobDate.replaceAll("'", "").length() == 6){
                    jobDate = "" + args[0] + "01";
                }

                boolean flag = checkDate(jobDate.replaceAll("'", ""));

                if (!flag) {
                    if(LOGGER.isInfoEnabled()) {
                        LOGGER.info("      ERROR!!");
                        LOGGER.info("      ��¥������ �߸��Ǿ����ϴ�.");
                        LOGGER.info("  =============================================================");
                    }
                }else{
                    Encrypt encrypt = new Encrypt();
                    dbInfo = propEnv.getProperty("DB_URL");
                    pmpUser = propEnv.getProperty("DB_USER_NAME");
                    pmpPass = encrypt.decryptAES256(propEnv.getProperty("DB_PASSWORD"));

                    String sql = "";
                    String getDdl = "";

                    try {
                        // ���� �α�, SQL ����
                        Process proclogrm = rt.exec("rm -f " + logDir + args[0] + "_" + args[1] + ".log");
                        Process procsqlrm = rt.exec("rm -f " + genDir + args[0] + "_" + args[1] + ".sql");

                        // �ߺ� ���� �۾� üũ
                        String cmd[] = {"sh", "-c", "ps -ef | grep PMPBatchrun | grep "+ args[1] + " | wc -l "};
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
                                  OracleDataSource ods = new OracleDataSource();
                                  ods.setURL(dbInfo);
                                  ods.setUser(pmpUser);
                                  ods.setPassword(pmpPass);

                                  connChk = ods.getConnection();

                                  sql = "SELECT /*+ FULL(T10) PARALLEL(4) */ COUNT(1) AS CNT FROM PMPOWN.WCPCM001TH T10 WHERE BAT_WK_PRRST_CD = 'R' AND BAT_WK_ID = '" + args[1] + "' ";
                                  pstmtChk = connChk.prepareStatement(sql);
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

                                  sql = ""; // initial Sql Query
                                } catch(Exception chke) {
                                  LOGGER.info("  " + chke.getMessage());
                                } finally {
                                  if(pstmtChk !=null) {try {pstmtChk.close();} catch(Exception conne) {}}
                                  if(connChk !=null) {try {connChk.close();} catch(Exception conne) {}}
                                }
                              }
                            }

                        } catch (Exception e) {
                            LOGGER.info("  " + e.getMessage());
                        } finally {
                            cbr.close();
                        }

                    } catch(Exception e) {
                        LOGGER.info("  " + e.getMessage());
                    } finally {
                    }

                    //.sql����
                    createSql(orgDir, args[1], runDir, genDir, args[0]);


                    // ���ν��� ���� ���� �� ��ȯ
                    FileReader fr = new FileReader(genDir + args[0] + "_" + args[1] + "_tmp.sql");
                    BufferedReader br = new BufferedReader(fr);
                    String str = "";
                    String newStr = "";

                try{
                    do{
                       str = br.readLine();

                       //���� ġȯ
                       if(str.indexOf("P_JOB_DATE") > -1){
                         str = str.replaceAll("P_JOB_DATE",jobDate);
                       }else if(str.indexOf("P_JOB_NAME") > -1){
                         str = str.replaceAll("P_JOB_NAME",jobName);
                       }else if(str.indexOf("P_PAR3_V") > -1){
                         str = str.replaceAll("P_PAR3_V",inParam3);
                       }else if(str.indexOf("P_PAR4_V") > -1){
                         str = str.replaceAll("P_PAR4_V",inParam4);
                       }else if(str.indexOf("P_PAR5_V") > -1){
                         str = str.replaceAll("P_PAR5_V",inParam5);
                       }else if(str.indexOf("P_PAR6_V") > -1){
                         str = str.replaceAll("P_PAR6_V",inParam6);
                       }else if(str.indexOf("P_FILE_NAME") > -1){
                         str = str.replaceAll("P_FILE_NAME",fileName);
                       }else if(str.indexOf("P_JOB_JUGI") > -1){
                         str = str.replaceAll("P_JOB_JUGI",jobJugi);
                       }else if(str.indexOf("P_JOB_LOG_NAME") > -1){
                         str = str.replaceAll("P_JOB_LOG_NMAE",jobLogName);
                       }else if(str.indexOf("P_L_DRIVER") > -1){
                         str = str.replaceAll("P_L_DRIVER",lDriver);
                       }
                       str = str.replaceAll("IN_BASE_DT",jobDate);
                       newStr += "\n" + str;
                    }while(str!=null);
                }catch(Exception e){
                     //LOGGER.info("  ");
                }finally {
                     br.close();
                     fr.close();
                }

                BufferedWriter out = new BufferedWriter(new FileWriter(genDir + args[0] + "_" + args[1] + ".sql"));
                
                newStr += "\n";
                
                out.write(newStr);
                out.newLine();
                out.close();
                
                OracleDataSource ods = new OracleDataSource();
                ods.setURL(dbInfo);
                ods.setUser(pmpUser);
                ods.setPassword(pmpPass);

                conn = ods.getConnection();

                step = conn.createStatement();

                /*------------------------------------------------------------------------------*/
                /* EXCHANG TABLE CREATE */
                /*------------------------------------------------------------------------------*/
                sql = " DECLARE P_RTN     VARCHAR2(10) := ''; ";
                sql += " BEGIN ";
                sql += " SP_EXC_TABLE_CR(P_RTN, 'PMPOWN', '" + oriTbName + "'); END;";

                step.executeUpdate(sql);


                sql = ""; // initial Sql Query

                try{
                    FileInputStream fin = new FileInputStream(genDir + args[0] + "_" + args[1] + ".sql");
                    br = new BufferedReader(new InputStreamReader(fin, "MS949"));
                    str = "";
                    try{
                        do{
                            str = br.readLine();
                            if(!str.equals("null")){
                                getDdl += "\n" + str;
                            }
                        }while(!(str == null));
                    }catch(Exception e){
                        if(e.getMessage() != null){
                            LOGGER.info("  Ddl Progress Error : " + e.getMessage());
                        }
                    }finally {
                      br.close();
                      fin.close();
                    }
                }catch(Exception e){
                    LOGGER.info(" ");
                }

                step.executeUpdate(getDdl);

                calendar = Calendar.getInstance();
                df = new SimpleDateFormat("yyyyMMdd");
                edDate = df.format(calendar.getTime());
                df = new SimpleDateFormat("HHmmss");
                edTime = df.format(calendar.getTime());


                totLog += "  =============================================================" + "\n";
                totLog += "                         <���α׷� ����>" + "\n";
                totLog += "    ���������        : " + args[0] + "\n";
                totLog += "    �����α׷���      : " + args[1] + "\n";
                totLog += "    ���۾����۽ð�    : " + stDate + stTime + "\n";
                totLog += "    ���۾�����ð�    : " + edDate + edTime + "\n";
                totLog += "    ���۾��ҿ�ð�    : " + changeTime(getTime(stDate + stTime, edDate + edTime) + "") + "\n";
                totLog += "  =============================================================" + "\n";
                
                LOGGER.info("  =============================================================");
                LOGGER.info("                         <���α׷� ����>");
                LOGGER.info("    ���������        : " + args[0]);
                LOGGER.info("    �����α׷���      : " + args[1]);
                LOGGER.info("    ���۾����۽ð�    : " + stDate + stTime);
                LOGGER.info("    ���۾�����ð�    : " + edDate + edTime);
                LOGGER.info("    ���۾��ҿ�ð�    : " + changeTime(getTime(stDate + stTime, edDate + edTime) + ""));
                LOGGER.info("  =============================================================");

                resChk = 0;
                }
            }
        }catch(Exception e){
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
        }finally{
            
            try{
                Process proc = rt.exec("rm -f " + genDir + args[0] + "_" + args[1] + "_tmp.sql");
                
                //out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logPath),"UTF8"));
                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logPath),"euc-kr"));
                out.write(totLog);
                out.newLine();
                out.close();
                
                if(errStr.indexOf("ORA-06550") > -1){
                
                   /*-----------------------------------------------------------------------------------*/
                   /* SQL ���� ERROR �߻� �� ���� �α� Procedure ȣ�� ����                              */
                   /*-----------------------------------------------------------------------------------*/
                
                   String sql = " DECLARE P_RTN           VARCHAR2(10)  :='';";
                   sql += " P_ALTER1                VARCHAR2(1000);";
                   sql += " P_ALTER2                VARCHAR2(1000);";
                   sql += " P_PMP_BAT_PGM_NM         VARCHAR2(100) := '';";
                   sql += " BEGIN ";
                   sql += "     P_ALTER1 := 'ALTER SESSION SET NLS_DATE_FORMAT      = \"YYYYMMDD\"'; ";
                   sql += "     P_ALTER2 := 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = \"YYYYMMDDHH24MISSFF2\"'; ";
                   sql += "     EXECUTE IMMEDIATE P_ALTER1; ";
                   sql += "     EXECUTE IMMEDIATE P_ALTER2; ";
                   sql += "     SELECT NVL(MAX(COMMENTS), ' ') || ' ��ġ ���α׷�' INTO P_PMP_BAT_PGM_NM FROM(SELECT COMMENTS FROM ALL_TAB_COMMENTS ";
                   sql += "     WHERE OWNER = 'PMPOWN' AND TABLE_NAME = '" + oriTbName + "' UNION ALL SELECT '" + args[1] + "' FROM DUAL) T10; ";
                   sql += "     SP_LOG_MRT('" + args[1] + "',P_PMP_BAT_PGM_NM,'" + args[0]
                       + "','000','Batchrun ���� ����','" + stDate + stTime + "00','" + stDate + stTime
                       + "00',0,'Ora-06550','Parsing Error','ERR','Y',P_RTN); END;";
                
                   step.executeUpdate(sql);
                
                }
                   /*-----------------------------------------------------------------------------------*/
                   /* SQL ���� ERROR �߻� �� ���� �α� Procedure ȣ�� ����                              */
                   /*-----------------------------------------------------------------------------------*/

                if(resChk == 0){
                    resChkStr = "success.";
                }else{
                    resChkStr = "fail.";
                }
                
                LOGGER.info("  Log File Path : " + logPath + "\t" + resChkStr);
                
                System.exit(resChk);
            
            }catch(Exception e){
                System.exit(1);
            }finally{
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
    }

    public static boolean checkDate(String i_date){
        boolean b = true;

        if(i_date.length() < 6 || i_date.length() == 7){
            return false;
        }

        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd", java.util.Locale.KOREA);

        df.setLenient(false);

        try{
            java.util.Date dt = df.parse(i_date);
        }catch(ParseException pe){
            b = false;
        }catch(IllegalArgumentException ae){
            b = false;
        }

        return b;
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

    public static void createSql(String orgDir, String jobName, String runDir, String genDir, String jobDate){
        String totalStr = "";
        String headName = "";
        String tailName = "";

        try{
            headName = "PMPHead.sql";
            tailName = "PMPTail.sql";

            FileReader fr = new FileReader(runDir + headName);
            BufferedReader br = new BufferedReader(fr);
            String str = "";
            String head = "";

            try{
                do{
                    str = br.readLine();
                    head += "\n" + str;
                }while(!(str == null));
            }catch(Exception e){
                LOGGER.info(" ");
            }finally {
                br.close();
                fr.close();
            }



            fr = new FileReader(orgDir + jobName + ".sql");
            br = new BufferedReader(fr);
            str = "";
            String body = "";
            try{
                do{
                    str = br.readLine();
                    body += "\n" + str;
                }while(!(str == null));
            }catch(Exception e){
                LOGGER.info(" ");
            }finally {
                br.close();
                fr.close();
            }

            fr = new FileReader(runDir + tailName);
            br = new BufferedReader(fr);
            str = "";
            String tail = "";
            try{
                do{
                    str = br.readLine();
                    tail += "\n" + str;
                }while(!(str == null));
            }catch(Exception e){
                LOGGER.info(" ");
            }finally {
                br.close();
                fr.close();
            }

            // �����ڵ� ������ UTF-8�� ���� �� ���� ���ۿ� ���鹮�� FEFF�� �����ϱ� ����
            if (head.indexOf("\uFEFF") > -1) {
                totalStr += head.substring(2);
            } else {
                totalStr += head;
            }

            totalStr += "\n";

            if (body.indexOf("\uFEFF") > -1) {
                totalStr += body.substring(2);
            } else {
                totalStr += body;
            }

            totalStr += "\n";

            if (tail.indexOf("\uFEFF") > -1) {
                totalStr += tail.substring(2);
            } else {
                totalStr += tail;
            }

            totalStr = totalStr.replaceAll("null", "");

            BufferedWriter out = null;
            try {
                out = new BufferedWriter(new FileWriter(genDir + jobDate + "_" + jobName + "_tmp.sql"));
                out.write(totalStr);
                out.newLine();
            } finally {
                if(out != null) out.close();
            }

            Runtime rt = Runtime.getRuntime();

            Process proc = rt.exec("chmod 777" + genDir + jobDate + "_" + jobName + "_tmp.sql");
        }catch(Exception e){
            LOGGER.info(" ");
        }
    }

    private static Properties getEnvProperties() {
        Properties prop = new Properties();

        String sbPropertiesPath = "/app/mk_batch/COM/ENV/PMPenv.ini";

        try(InputStream input = new FileInputStream(sbPropertiesPath.toString())){
            prop.load(input);
        } catch(IOException e) {
            LOGGER.info(" ");
        }

        return prop;
    }
}