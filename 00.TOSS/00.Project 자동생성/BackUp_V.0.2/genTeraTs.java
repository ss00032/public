import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class genTeraTs {

	public String TeraTs() {
		Connection connCbs = null;
		PreparedStatement pstmCbs = null;
		PreparedStatement pstmCbsPart = null;
		ResultSet cbsRs = null;
		ResultSet cbsPartRs = null;
		ResultSetMetaData count = null;
		PreparedStatement partcbs = null;
		Connection connsor = null;
		PreparedStatement pstmsor = null;
		PreparedStatement pstmsorPart = null;
		ResultSet sorRs = null;
		ResultSet sorPartRs = null;

		try {
			String cbsquery = "SELECT  \r"
					+ "                     A.OWNER\r"
					+ "                     ,A.TABLE_NAME\r"
					+ "                     ,B.COMMENTS AS TABLE_COMMENTS\r"
					+ "                     ,C.COMMENTS AS COLUMN_COMMENTS\r"
					+ "                     ,A.DATA_TYPE\r"
					+ "                     ,A.DATA_LENGTH\r"
					+ "                     ,A.DATA_PRECISION\r"
					+ "                     ,A.DATA_SCALE\r"
					+ "                     ,A.NULLABLE\r"
					+ "                     ,A.COLUMN_ID\r"
					+ "                     ,A.DEFAULT_LENGTH\r"
					+ "                     ,TRIM(REPLACE(DBMS_XMLGEN.GETXMLTYPE(\u0027SELECT DATA_DEFAULT FROM ALL_TAB_COLS WHERE OWNER=\u0027\u0027\u0027||A.OWNER||\u0027\u0027\u0027 AND TABLE_NAME = \u0027\u0027\u0027||A.TABLE_NAME||\u0027\u0027\u0027 AND COLUMN_NAME = \u0027\u0027\u0027||A.COLUMN_NAME||\u0027\u0027\u0027 \u0027).EXTRACT(\u0027//text()\u0027),\u0027&apos;\u0027,\u0027\u0027\u0027\u0027)) AS DATA_DEFAULT\r"
					+ "                     ,(CASE WHEN D.COLUMN_NAME IS NULL THEN \u0027N\u0027 ELSE \u0027Y\u0027 END) AS PK_YN\r"
					+ "            FROM ALL_TAB_COLS A\r"
					+ "            LEFT \r"
					+ "             JOIN ALL_TAB_COMMENTS B\r"
					+ "                  ON B.OWNER = A.OWNER\r"
					+ "               AND B.TABLE_NAME = A.TABLE_NAME\r"
					+ "              LEFT \r"
					+ "              JOIN ALL_COL_COMMENTS C\r"
					+ "                 ON C.OWNER = A.OWNER\r"
					+ "              AND C.TABLE_NAME = A.TABLE_NAME\r"
					+ "               AND C.COLUMN_NAME = A.COLUMN_NAME\r"
					+ "              LEFT \r"
					+ "              JOIN ALL_IND_COLUMNS D \r"
					+ "                   ON D.TABLE_OWNER = A.OWNER\r"
					+ "                  AND D.TABLE_NAME = A.TABLE_NAME\r"
					+ "                  AND D.COLUMN_NAME = A.COLUMN_NAME\r"
					+ "                   AND (D.INDEX_NAME LIKE \u0027PK%\u0027 OR D.INDEX_NAME LIKE \u0027%PK\u0027)\r"
					+ "               WHERE A.OWNER = \u0027CBSOWN\u0027\r"
					+ "                   AND A.TABLE_NAME = ?\r"
					+ "                   AND A.COLUMN_NAME NOT LIKE \u0027%PSWD\u0027\r"
					+ "                   AND A.HIDDEN_COLUMN = \u0027NO\u0027\r"
					+ "                ORDER BY A.OWNER, A.TABLE_NAME, A.COLUMN_ID";
			int rowIndex = 2;
			int allChangeCount = 0;
			int notChangeCount = 0;
			int delTableCount = 0;
			int ifTableCount = 0;
			int snpTableCount = 0;
			
			FileInputStream file = new FileInputStream("엑셀경로");
			XSSFWorkbook workBook = new XSSFWorkbook(file);
			XSSFSheet sheet = workBook.getSheet("시트명");
			int rows = sheet.getPhysicalNumberOfRows();
			int chgCount = 0;
			
			connCbs = cbsDBConnection.getConnection();
			connsor = sorDBConnection.getConnection();
			
			for (rowIndex = 2; rowIndex < rows; rowIndex++) {
				
				XSSFRow row = sheet.getRow(rowIndex);
				XSSFCell tblno = row.getCell(0);
				XSSFCell tblID = row.getCell(5);
				XSSFCell tblNM = row.getCell(6);
				XSSFCell tblgb = row.getCell(7);
				XSSFCell tblpart = row.getCell(16);
				XSSFCell tblloadchk = row.getCell(17);
				XSSFCell tblSnap = row.getCell(23);
				XSSFCell tblSub = row.getCell(26);
				XSSFCell cbstbl = row.getCell(21);
				XSSFCell sUCD = row.getCell(32);
				XSSFCell sLCD = row.getCell(35);
				XSSFCell sUID = row.getCell(38);
				XSSFCell sLID = row.getCell(41);
				XSSFCell sUCDnm = row.getCell(33);
				XSSFCell sLCDnm = row.getCell(36);
				XSSFCell sUIDnm = row.getCell(39);
				XSSFCell sLIDnm = row.getCell(42);
				XSSFCell dsnGb = row.getCell(44);

				String tblid = tblID.getStringCellValue().trim();
				String snpschk = tblpart.getStringCellValue().trim();
                String gbchk = tblgb.getStringCellValue().trim();
                String tblname = tblNM.getStringCellValue();
                String tblsnps = tblSnap.getStringCellValue();
                String tblName = tblNM.getStringCellValue();
                String sub = tblSub.getStringCellValue().trim();
                String UCD = sUCD.getStringCellValue();
                String LCD = sLCD.getStringCellValue();
                String UID = sUID.getStringCellValue();
                String LID = sLID.getStringCellValue();
                String UCDnm = sUCDnm.getStringCellValue();
                String LCDnm = sLCDnm.getStringCellValue();
                String UIDnm = sUIDnm.getStringCellValue();
                String LIDnm = sLIDnm.getStringCellValue();
                String DSNgb = dsnGb.getStringCellValue();
                
                String loadType = "CHG";
                String loadType2 = "INI";

                Date time = new Date();

                SimpleDateFormat JOB_DT_format = new SimpleDateFormat("yyyyMMdd_HHmm");
                SimpleDateFormat CR_DT_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                String JOB_DT = JOB_DT_format.format(time);
                String CR_DT = CR_DT_format.format(time);

                String GRP_INI = "";

                if(sub.equals("DOP")) {
                	GRP_INI = "60";
                } else if(sub.equals("DPD")) {
                	GRP_INI = "61";
                } else if(sub.equals("DPL")) {
                	GRP_INI = "62";
                } else if(sub.equals("DPV")) {
                	GRP_INI = "67";
                } else if(sub.equals("DPG")) {
                	GRP_INI = "68";
                } else if(sub.equals("DHC")) {
                	GRP_INI = "71";
                } else if(sub.equals("DEC")) {
                	GRP_INI = "83";
                } else if(sub.equals("DRU")) {
                	GRP_INI = "77";
                } else if(sub.equals("DQH")) {
                	GRP_INI = "79";
                } else if(sub.equals("DMM")) {
                	GRP_INI = "81";
                } else if(sub.equals("DPF")) {
                	GRP_INI = "87";
                } else if(sub.equals("DCA")) {
                	GRP_INI = "94";
                }

                String GRP_CHG = "";

                if(sub.equals("DOP")) {
                	GRP_CHG = "63";
                } else if(sub.equals("DPD")) {
                	GRP_CHG = "64";
                } else if(sub.equals("DPL")) {
                	GRP_CHG = "65";
                } else if(sub.equals("DPV")) {
                	GRP_CHG = "66";
                } else if(sub.equals("DPG")) {
                	GRP_CHG = "69";
                } else if(sub.equals("DHC")) {
                	GRP_CHG = "70";
                } else if(sub.equals("DEC")) {
                	GRP_CHG = "82";
                } else if(sub.equals("DRU")) {
                	GRP_CHG = "76";
                } else if(sub.equals("DQH")) {
                	GRP_CHG = "78";
                } else if(sub.equals("DMM")) {
                	GRP_CHG = "80";
                } else if(sub.equals("DPF")) {
                	GRP_CHG = "86";
                } else if(sub.equals("DCA")) {
                	GRP_CHG = "95";
                }

                if (gbchk.equals("삭제")) {
                	delTableCount++;
                	continue;
                }
                
                String loadchk = tblloadchk.getStringCellValue().trim();
                
                if (loadchk.equals("I/F적재형")) {
                	ifTableCount++;
                	continue;
                }
                
                if (tblsnps.equals("Y")) {
                	
                	String sPgmPath = "D:\\\\#PGM\\\\#작업\\\\java\\\\TS\\\\CHG\\\\TSP_" + LCD + ".ts";
                	String sSrcPgmPath0010 = "D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\" + sub + "\\\\" + LCD + "_US_0010.sql";
                	String sSrcPgmPath0020 = "D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\" + sub + "\\\\" + LCD + "_US_0020.sql";
                	String sSrcPgmPath0030 = "D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\" + sub + "\\\\" + LCD + "_US_0030.sql";

                	String sDescUS0010 = "스냅샷 파티션 Truncate";
                	String sDescUS0020 = "스냅샷 Insert";
                	String sDescUS0030 = "스냅샷 파티션 통계 생성";

                	String sUS_0010 = "";
                	String sUS_0020 = "";
                	String sUS_0030 = "";

                	File file0010 = new File(sSrcPgmPath0010);
                	if (!file0010.exists()) {
                		System.out.println("[" + tblno + "]" + "[" + tblid + "]" + "[" + tblname + "]" + "[" + LCD + "]" + " 파일이 없어!");
                	} else {
                		FileInputStream fileInStream = new FileInputStream(file0010);
                		InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
                		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                		int ch;

                		while((ch = bufferedReader.read()) != -1) {
                			sUS_0010 += (char)ch;
                		}
                	}

                		File file0020 = new File(sSrcPgmPath0020);
                		if (!file0020.exists()) {
                			System.out.println("[" + tblno + "]" + "[" + tblid + "]" + "[" + tblname + "]" + "[" + LCD + "]" + " 파일이 없어!");
                		} else {
                			FileInputStream fileInStream = new FileInputStream(file0020);
                			InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
                			BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                			int ch;

                			while((ch = bufferedReader.read()) != -1) {
                				sUS_0020 += (char)ch;
                			}
                		}

                		File file0030 = new File(sSrcPgmPath0030);
                		if (!file0030.exists()) {
                			System.out.println("[" + tblno + "]" + "[" + tblid + "]" + "[" + tblname + "]" + "[" + LCD + "]" + " 파일이 없어!");
                		} else {
                			FileInputStream fileInStream = new FileInputStream(file0030);
                			InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
                			BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                			int ch;

                			while((ch = bufferedReader.read()) != -1) {
                				sUS_0030 += (char)ch;
                			}
                		}

                String sTs = "file=tsscm1.2"
                + "/prj_grp " + GRP_CHG + " adwsor /ADW/" + loadType + "/" + sub + "\r"
                + "$prj 0\r"
                + ".prj_id=\"1\"\r"
                + "*prj_grp_id=\"" + GRP_CHG + "\"\r"
                + "!nm=\"" + LCD + "\"\r"
                + ".desc11=\"" + LCDnm + "\"\r"
                + ".cur_ver_id=\"2\"\r"
                + ".owner_nm=\"adwsor\"\r"
                + ".cr_dt=\"" + CR_DT + "\"\r"
                + ".up_dt=\"" + CR_DT + "\"\r"
                + ".perm=\"774\"\r"
                + ".grp_nm=\"adwsor\"\r"
                + ".favorite_yn=\"N\"\r"
                + ".status_cd=\"0\"\r"
                + ".sms_id=\"0\"\r"
                + ".email_id=\"0\"\r"
                + "$+\r"
                + "$$\r"
                + "$arrow_dtl 4\r"
                + ".prj_id=\"1\"\r"
                + ".prj_ver_id=\"1\"\r"
                + ".block_id=\"2\"\r"
                + ".sequence=\"1\"\r"
                + ".sequence2=\"1\"\r"
                + ".pos_x=\"82\"\r"
                + ".pos_y=\"249\"\r"
                + "$+\r"
                + ".prj_id=\"1\"\r"
                + ".prj_ver_id=\"1\"\r"
                + ".block_id=\"2\"\r"
                + ".sequence=\"1\"\r"
                + ".sequence2=\"2\"\r"
                + ".pos_x=\"166\"\r"
                + ".pos_y=\"249\"\r"
                + "$+\r"
                + ".prj_id=\"1\"\r"
                + ".prj_ver_id=\"1\"\r"
                + ".block_id=\"3\"\r"
                + ".sequence=\"2\"\r"
                + ".sequence2=\"1\"\r"
                + ".pos_x=\"202\"\r"
                + ".pos_y=\"248\"\r"
                + "$+\r"
                + ".prj_id=\"1\"\r"
                + ".prj_ver_id=\"1\"\r"
                + ".block_id=\"3\"\r"
                + ".sequence=\"2\"\r"
                + ".sequence2=\"2\"\r"
                + ".pos_x=\"286\"\r"
                + ".pos_y=\"248\"\r"
                + "$+\r"
                + "$$\r"
                + "$block 4\r"
                + ".prj_id=\"1\"\r"
                + ".prj_ver_id=\"1\"\r"
                + ".block_id=\"2\"\r"
                + ".nm=\"US_0010\"\r"
                + ".block_kind_cd=\"11\"\r"
                + ".block_status_cd=\"1\"\r"
                + ".parallel=\"1\"\r"
                + ".comp=\"N\"\r"
                + ".pos_x=\"30\"\r"
                + ".pos_y=\"230\"\r"
                + ".desc1=\"" + sDescUS0010 + "\"\r"
                + ".cr_dt=\"" + CR_DT + "\"\r"
                + ".up_dt=\"" + CR_DT + "\"\r"
                + ".block_kind_before=\"0\"\r"
                + ".block_kind_arter=\"0\"\r"
                + "$+\r"
                + ".prj_id=\"1\"\r"
                + ".prj_ver_id=\"1\"\r"
                + ".block_id=\"3\"\r"
                + ".nm=\"US_0020\"\r"
                + ".block_kind_cd=\"11\"\r"
                + ".block_status_cd=\"1\"\r"
                + ".parallel=\"1\"\r"
                + ".comp=\"N\"\r"
                + ".pos_x=\"150\"\r"
                + ".pos_y=\"230\"\r"
                + ".desc1=\"" + sDescUS0020 + "\"\r"
                + ".cr_dt=\"" + CR_DT + "\"\r"
                + ".up_dt=\"" + CR_DT + "\"\r"
                + ".block_kind_before=\"0\"\r"
                + ".block_kind_arter=\"0\"\r"
                + "$+\r"
                + ".prj_id=\"1\"\r"
                + ".prj_ver_id=\"1\"\r"
                + ".block_id=\"4\"\r"
                + ".nm=\"US_0030\"\r"
                + ".block_kind_cd=\"11\"\r"
                + ".block_status_cd=\"1\"\r"
                + ".parallel=\"1\"\r"
                + ".comp=\"N\"\r"
                + ".pos_x=\"270\"\r"
                + ".pos_y=\"230\"\r"
                + ".desc1=\"" + sDescUS0030 + "\"\r"
                + ".cr_dt=\"" + CR_DT + "\"\r"
                + ".up_dt=\"" + CR_DT + "\"\r"
                + ".block_kind_before=\"0\"\r"
                + ".block_kind_arter=\"0\"\r"
                + "$+\r"
                + "$$\r"
                + "$dep 4\r"
                + ".prj_id=\"1\"\r"
                + ".prj_ver_id=\"1\"\r"
                + ".block_id=\"2\"\r"
                + ".sequence=\"1\"\r"
                + ".dep_type_cd=\"1\"\r"
                + ".target_block_id=\"3\"\r"
                + ".link_kind_cd=\"1\"\r"
                + ".link_type_cd=\"1\"\r"
                + "$+\r"
                + ".prj_id=\"1\"\r"
                + ".prj_ver_id=\"1\"\r"
                + ".block_id=\"3\"\r"
                + ".sequence=\"2\"\r"
                + ".dep_type_cd=\"1\"\r"
                + ".target_block_id=\"4\"\r"
                + ".link_kind_cd=\"1\"\r"
                + ".link_type_cd=\"1\"\r"
                + "$+\r"
                + "$$\r"
                + "$procedure_block 4\r"
                + ".prj_id=\"1\"\r"
                + ".prj_ver_id=\"1\"\r"
                + ".block_id=\"2\"\r"
                + ".db_nm=\"SOR\"\r"
                + ".nm=\"US_0010\"\r"
                + ".desc1=\"" + sDescUS0010 + "\"\r"
                + ".sql_source=\"set serveroutput on\r"
                + "Declare\r"
                + "      o_result int;\r"
                + "      o_errmsg varchar2(4000);\r"
                + "\r"
                + "BEGIN\r"
                + sUS_0010 + "\r"
                + "     dbms_output.put_line(\u0027o_result : \u0027|| o_result);\r"
                + "     dbms_output.put_line(\u0027o_errmsg : \u0027|| o_errmsg);\r"
                + "END;\r"
                + "\"\r"
                + ".sp_sql=\"1\"\r"
                + ".sp1=\"0\"\r"
                + ".sp2=\"1\"\r"
                + ".sp3=\"0\"\r"
                + ".sp4=\"0\"\r"
                + ".sp_text=\"" + sUS_0010 + "\"\r"
                + ".sysbase_native_dsn=\"\"\r"
                + ".sp_option=\"\"\r"
                + ".ifiller1=\"1\"\r"
                + ".ifiller2=\"0\"\r"
                + ".ifiller3=\"0\"\r"
                + ".sfiller1=\"\"\r"
                + ".sfiller2=\"\"\r"
                + ".sfiller3=NULL\r"
                + ".stdout_file=NULL\r"
                + ".stderr_file=NULL\r"
                + "$+\r"
                + ".prj_id=\"1\"\r"
                + ".prj_ver_id=\"1\"\r"
                + ".block_id=\"3\"\r"
                + ".db_nm=\"SOR\"\r"
                + ".nm=\"US_0020\"\r"
                + ".desc1=\"" + sDescUS0020 + "\"\r"
                + ".sql_source=\"set serveroutput on\r"
                + "Declare\r"
                + "      o_result int;\r"
                + "      o_errmsg varchar2(4000);\r"
                + "      o_count int;\r"
                + "BEGIN\r"
                + sUS_0020 + "\r"
                + "     dbms_output.put_line(\u0027o_result : \u0027|| o_result);\r"
                + "     dbms_output.put_line(\u0027o_errmsg : \u0027|| o_errmsg);\r"
                + "     dbms_output.put_line(\u0027o_count : \u0027|| o_count);\r"
                + "END;\r"
                + "\"\r"
                + ".sp_sql=\"1\"\r"
                + ".sp1=\"0\"\r"
                + ".sp2=\"1\"\r"
                + ".sp3=\"1\"\r"
                + ".sp4=\"0\"\r"
                + ".sp_text=\"" + sUS_0020 + "\"\r"
                + ".sysbase_native_dsn=\"\"\r"
                + ".sp_option=\"\"\r"
                + ".ifiller1=\"1\"\r"
                + ".ifiller2=\"0\"\r"
                + ".ifiller3=\"0\"\r"
                + ".sfiller1=\"\"\r"
                + ".sfiller2=\"\"\r"
                + ".sfiller3=NULL\r"
                + ".stdout_file=NULL\r"
                + ".stderr_file=NULL\r"
                + "$+\r"
                + ".prj_id=\"1\"\r"
                + ".prj_ver_id=\"1\"\r"
                + ".block_id=\"4\"\r"
                + ".db_nm=\"SOR\"\r"
                + ".nm=\"US_0030\"\r"
                + ".desc1=\"" + sDescUS0030 + "\"\r"
                + ".sql_source=\"set serveroutput on\r"
                + "Declare\r"
                + "      o_result int;\r"
                + "      o_errmsg varchar2(4000);\r"
                + "      o_count int;\r"
                + "BEGIN\r"
                + sUS_0030 + "\r"
                + "     dbms_output.put_line(\u0027o_result : \u0027|| o_result);\r"
                + "     dbms_output.put_line(\u0027o_errmsg : \u0027|| o_errmsg);\r"
                + "     dbms_output.put_line(\u0027o_count : \u0027|| o_count);\r"
                + "END;\r"
                + "\"\r"
                + ".sp_sql=\"1\"\r"
                + ".sp1=\"0\"\r"
                + ".sp2=\"1\"\r"
                + ".sp3=\"1\"\r"
                + ".sp4=\"0\"\r"
                + ".sp_text=\"" + sUS_0030 + "\"\r"
                + ".sysbase_native_dsn=\"\"\r"
                + ".sp_option=\"\"\r"
                + ".ifiller1=\"1\"\r"
                + ".ifiller2=\"0\"\r"
                + ".ifiller3=\"0\"\r"
                + ".sfiller1=\"\"\r"
                + ".sfiller2=\"\"\r"
                + ".sfiller3=NULL\r"
                + ".stdout_file=NULL\r"
                + ".stderr_file=NULL\r"
                + "$+\r"
                + "$$\r"
                + "$prj_ver 4\r"
                + ".prj_id=\"1\"\r"
                + ".ver_nm=\"\"\r"
                + ".desc1=\"<viewsize>1000;1000</viewsize>\"\r"
                + ".cr_dt=\"" + CR_DT + "\"\r"
                + ".up_dt=\"" + CR_DT + "\"\r"
                + "$+\r"
                + "$$\r"
                + "$note_memo 4\r"
                + ".prj_id=\"1\"\r"
                + ".prj_ver_id=\"1\"\r"
                + ".block_id=\"1\"\r"
                + ".nm=\"프로젝트 정보\"\r"
                + ".block_kind_cd=\"13\"\r"
                + ".block_status_cd=\"1\"\r"
                + ".pos_x=\"787404\"\r"
                + ".pos_y=\"786610\"\r"
                + ".desc1=\"bsEllipse\r"
                + "1. 프로젝트 명 : " + LCD + " - " + LCDnm + "\r"
                + "2. 변경이력\r"
                + "     일자                      작성자                 변경내용\r"
                + "     ---------------  -------------   -----------------------------------------------------------------------------------------------\r"
                + "     2020-09-09    ADW                   최초작성\"\r"
                + ".cr_dt=\"" + CR_DT + "\"\r"
                + ".up_dt=\"" + CR_DT + "\"\r"
                + ".block_kind_before=\"0\"\r"
                + ".block_kind_after=\"0\"\r"
                + "$+\r"
                + "$$\r"
                + "";


                File TSfile = new File(sPgmPath);

                if (!TSfile.exists()) {

                	FileOutputStream fileOutStream = new FileOutputStream(sPgmPath);
                	OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
                	BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
                	bufferedWriter.write(sTs);
                	bufferedWriter.close();
                	chgCount ++;
                } else {
                	FileInputStream fileInStream = new FileInputStream(sPgmPath);
                	InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
                	BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                	int ch;
                	String TSfileoldFile = "";

                	while((ch = bufferedReader.read()) != -1) {

                		TSfileoldFile += (char)ch;
                	}

                	String tsFileOldFileBe = TSfileoldFile.replaceAll("202[0-9].{15}", "");
                	String sTsBe = sTs.replaceAll("202[0-9].{15}", "");

                	if (tsFileOldFileBe.equals(sTsBe)) {

                	} else if (!tsFileOldFileBe.equals(sTsBe) && TSfile.exists()) {
                		FileOutputStream fileOutStream = new FileOutputStream(sPgmPath);
                		OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
                		BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
                		bufferedWriter.write(sTs);
                		bufferedWriter.close();
                		chgCount ++;
                	}
                }
                }

                if(!UCD.equals(null)&&!UID.equals(null)&&!tblsnps.equals("Y"))

                {

                	String sPgmchgPath = "D:\\\\#PGM\\\\#작업\\\\java\\\\TS\\\\CHG\\\\TSP_" + UCD + ".ts";
                	String sPgminiPath = "D:\\\\#PGM\\\\#작업\\\\java\\\\TS\\\\INI\\\\TSP_" + UID + ".ts";
                	String sSrcPgmchgPath0010 = "D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\" + sub + "\\\\" + UCD + "_UN_0010.sql";
                	String sSrcPgminiPath0010 = "D:\\\\#PGM\\\\#작업\\\\java\\\\INI\\\\" + sub + "\\\\" + UID + "_UN_0010.sql";
		
                	String sDsn = "BCV";

                	if (!DSNgb.equals(null) && !DSNgb.equals("")) {
                		sDsn = DSNgb;
                	}
                	
                	String sDescINIUS0010 = "초기적재 UNLOAD";
                	String sDescCHGUS0010 = "변경적재 UNLOAD";

                	String sChgUS_0010 = "";
                	String sIniUS_0010 = "";

                	File fileCHG = new File(sSrcPgmchgPath0010);
                	if (!fileCHG.exists()) {
                		System.out.println("[" + tblno + "]" + "[" + tblid + "]" + "[" + tblname + "]" + "[" + UCD + "]" + " 파일이 없어!");
                	} else {
                		FileInputStream fileInStream = new FileInputStream(sSrcPgmchgPath0010);
                		InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
                		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                		int ch;

                		while((ch = bufferedReader.read()) != -1) {
                			sChgUS_0010 += (char)ch;
                		}
                	}

                	File fileINI = new File(sSrcPgminiPath0010);
                	if (!fileINI.exists()) {
                		System.out.println("[" + tblno + "]" + "[" + tblid + "]" + "[" + tblname + "]" + "[" + UID + "]" + " 파일이 없어!");
                	} else {
                		FileInputStream fileInStream = new FileInputStream(sSrcPgminiPath0010);
                		InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
                		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                		int ch;

                		while((ch = bufferedReader.read()) != -1) {
                			sIniUS_0010 += (char)ch;
                		}
                	}

                	String INI = "file=tsscm1.2\r"
                			+ "/prj_grp " + GRP_INI + " adwsor /ADW/" + loadType2 + "/" + sub + "\r"
                			+ "$prj 0\r"
                			+ ".prj_id=\"1\"\r"
                			+ "*prj_grp_id=" + GRP_INI + "\"\r"
                			+ "!nm=" + UID + "\"\r"
                			+ ".desc11=" + UIDnm + "\"\r"
                			+ ".cur_ver_id=\"1\"\r"
                			+ ".owner_nm=\"adwsor\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".up_dt=" + CR_DT + "\"\r"
                			+ ".perm=\"774\"\r"
                			+ ".grp_nm=\"adwsor\"\r"
                			+ ".favorite_yn=\"N\"\r"
                			+ ".status_cd=\"0\"\r"
                			+ ".sms_id=\"0\"\r"
                			+ ".email_id=\"0\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$block 4\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"2\"\r"
                			+ ".nm=\"UN_0010\"\r"
                			+ ".block_kind_cd=\"8\"\r"
                			+ ".block_status_cd=\"5\"\r"
                			+ ".parallel=\"1\"\r"
                			+ ".comp=\"N\"\r"
                			+ ".pos_x=\"30\"\r"
                			+ ".pos_y=\"230\"\r"
                			+ ".desc1=" + sDescINIUS0010 + "\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".up_dt=" + CR_DT + "\"\r"
                			+ ".block_kind_before=\"0\"\r"
                			+ ".block_kind_after=\"0\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$unload2 4\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"2\"\r"
                			+ ".db_nm=" + sDsn + "\"\r"
                			+ ".ffd_id=\"20\"\r"
                			+ ".db_type_cd=\"2\"\r"
                			+ ".sybase_native_dsn=\"\r"
                			+ ".user_db_nm=\"\r"
                			+ ".user_nm=\"\r"
                			+ ".passwd=\"\r"
                			+ ".query=" + sIniUS_0010 + "\"\r"
                			+ ".outfile1=\"$$[SOR_" + loadType2 + "_SAM]/" + sub + "/" + tblid + ".dat\"\r"
                			+ ".type-\"2\"\r"
                			+ ".delim=\"|^,\"\r"
                			+ ".field1=\"\r"
                			+ ".field2=\"1024\r"
                			+ ".field3=\"\r"
                			+ ".field4=\"YYYYMMDD\r"
                			+ ".field5=\"\r"
                			+ ".field6=\"\r"
                			+ ".field7=\"\r"
                			+ ".field8=\"Y\r"
                			+ ".field9=\"\"\r"
                			+ ".connect_type=\"1\"\r"
                			+ ".sybase_quote_yn=\"N\"\r"
                			+ ".ffd_use_yn=\"N\"\r"
                			+ ".table_ffd_id=\"0\"\r"
                			+ ".line_delim_yn=\"N\"\r"
                			+ ".ifiller1=\"0\"\r"
                			+ ".ifiller2=\"0\"\r"
                			+ ".ifiller3=\"0\"\r"
                			+ ".sfiller1=\"\"\r"
                			+ ".sfiller2=\"\"\r"
                			+ ".sfiller3=\"\"\r"
                			+ ".ifiller4=\"0\"\r"
                			+ ".ifiller5=\"0\"\r"
                			+ ".ifiller6=\"1\"\r"
                			+ ".sfiller4=\"\"\r"
                			+ ".sfiller5=\"\"\r"
                			+ ".sfiller6=\"\"\r"
                			+ ".options=\"\r"
                			+ ".data_type_cd=\"1\"\r"
                			+ ".dfs_nm=\"LOCAL\"\r"
                			+ ".timestamp_format=\"YYYYMMDDHH24MISSFF6\"\r"
                			+ ".time_format=\"\r"
                			+ ".enf_row_delim=NULL\r"
                			+ ".use_rsc=\"N\"\r"
                			+ ".rsc_options=\"\r"
                			+ ".sqoop_use=\"0\"\r"
                			+ ".cond_clause=\"\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$note_memo 4\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"1\"\r"
                			+ ".nm=\"프로젝트 정보\"\r"
                			+ ".block_kind_cd=\"13\"\r"
                			+ ".block_status_cd=\"1\"\r"
                			+ ".pos_x=\"787404\"\r"
                			+ ".pos_y=\"786610\"\r"
                			+ ".desc1=\"bsEllipse\r"
                			+ "1. 프로젝트 명 : " + UID + " - " + UIDnm + "\r"
                			+ "2. 변경이력\r"
                			+ "     일자                      작성자                 변경내용\r"
                			+ "     ---------------  -------------   -----------------------------------------------------------------------------------------------\r"
                			+ "     2020-09-09    ADW                   최초작성\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".up_dt=" + CR_DT + "\"\r"
                			+ ".block_kind_before=\"0\"\r"
                			+ ".block_kind_after=\"0\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "";

                	String CHG = "file=tsscm1.2\r"
                			+ "/prj_grp " + GRP_CHG + " adwsor /ADW/" + loadType + "/" + sub + "\r"
                			+ "$prj 0\r"
                			+ ".prj_id=\"1\"\r"
                			+ "*prj_grp_id=" + GRP_CHG + "\"\r"
                			+ "!nm=" + UCD + "\"\r"
                			+ ".desc11=" + UCDnm + "\"\r"
                			+ ".cur_ver_id=\"1\"\r"
                			+ ".owner_nm=\"adwsor\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".up_dt=" + CR_DT + "\"\r"
                			+ ".perm=\"774\"\r"
                			+ ".grp_nm=\"adwsor\"\r"
                			+ ".favorite_yn=\"N\"\r"
                			+ ".status_cd=\"0\"\r"
                			+ ".sms_id=\"0\"\r"
                			+ ".email_id=\"0\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$block 4\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"2\"\r"
                			+ ".nm=\"UN_0010\"\r"
                			+ ".block_kind_cd=\"8\"\r"
                			+ ".block_status_cd=\"5\"\r"
                			+ ".parallel=\"1\"\r"
                			+ ".comp=\"N\"\r"
                			+ ".pos_x=\"30\"\r"
                			+ ".pos_y=\"230\"\r"
                			+ ".desc1=" + sDescCHGUS0010 + "\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".up_dt=" + CR_DT + "\"\r"
                			+ ".block_kind_before=\"0\"\r"
                			+ ".block_kind_after=\"0\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$unload2 4\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"2\"\r"
                			+ ".db_nm=" + sDsn + "\"\r"
                			+ ".ffd_id=\"20\"\r"
                			+ ".db_type_cd=\"2\"\r"
                			+ ".sybase_native_dsn=\"\r"
                			+ ".user_db_nm=\"\r"
                			+ ".user_nm=\"\r"
                			+ ".passwd=\"\r"
                			+ ".query=" + sChgUS_0010 + "\"\r"
                			+ ".outfile1=\"$$[SOR_" + loadType + "_SAM]/" + sub + "/" + tblid + ".dat\"\r"
                			+ ".type-\"2\"\r"
                			+ ".delim=\"|^,\"\r"
                			+ ".field1=\"\r"
                			+ ".field2=\"1024\r"
                			+ ".field3=\"\r"
                			+ ".field4=\"YYYYMMDD\r"
                			+ ".field5=\"\r"
                			+ ".field6=\"\r"
                			+ ".field7=\"\r"
                			+ ".field8=\"Y\r"
                			+ ".field9=\"\"\r"
                			+ ".connect_type=\"1\"\r"
                			+ ".sybase_quote_yn=\"N\"\r"
                			+ ".ffd_use_yn=\"N\"\r"
                			+ ".table_ffd_id=\"0\"\r"
                			+ ".line_delim_yn=\"N\"\r"
                			+ ".ifiller1=\"0\"\r"
                			+ ".ifiller2=\"0\"\r"
                			+ ".ifiller3=\"0\"\r"
                			+ ".sfiller1=\"\"\r"
                			+ ".sfiller2=\"\"\r"
                			+ ".sfiller3=\"\"\r"
                			+ ".ifiller4=\"0\"\r"
                			+ ".ifiller5=\"0\"\r"
                			+ ".ifiller6=\"1\"\r"
                			+ ".sfiller4=\"\"\r"
                			+ ".sfiller5=\"\"\r"
                			+ ".sfiller6=\"\"\r"
                			+ ".options=\"\r"
                			+ ".data_type_cd=\"1\"\r"
                			+ ".dfs_nm=\"LOCAL\"\r"
                			+ ".timestamp_format=\"YYYYMMDDHH24MISSFF6\"\r"
                			+ ".time_format=\"\r"
                			+ ".enf_row_delim=NULL\r"
                			+ ".use_rsc=\"N\"\r"
                			+ ".rsc_options=\"\r"
                			+ ".sqoop_use=\"0\"\r"
                			+ ".cond_clause=\"\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$note_memo 4\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"1\"\r"
                			+ ".nm=\"프로젝트 정보\"\r"
                			+ ".block_kind_cd=\"13\"\r"
                			+ ".block_status_cd=\"1\"\r"
                			+ ".pos_x=\"787404\"\r"
                			+ ".pos_y=\"786610\"\r"
                			+ ".desc1=\"bsEllipse\r"
                			+ "1. 프로젝트 명 : " + UCD + " - " + UCDnm + "\r"
                			+ "2. 변경이력\r"
                			+ "     일자                      작성자                 변경내용\r"
                			+ "     ---------------  -------------   -----------------------------------------------------------------------------------------------\r"
                			+ "     2020-09-09    ADW                   최초작성\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".up_dt=" + CR_DT + "\"\r"
                			+ ".block_kind_before=\"0\"\r"
                			+ ".block_kind_after=\"0\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "";

                	File TSUIDfile = new File(sPgminiPath);

                	if (!TSUIDfile.exists()) {

                		FileOutputStream fileOutStream = new FileOutputStream(sPgminiPath);
                		OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
                		BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
                		bufferedWriter.write(INI);
                		bufferedWriter.close();
                		chgCount ++;
                	} else {
                		FileInputStream fileInStream = new FileInputStream(sPgminiPath);
                		InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
                		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                		int ch;
                		String TSUIDfileoldFile = "";

                		while((ch = bufferedReader.read()) != -1) {

                			TSUIDfileoldFile += (char)ch;
                		}

                		String tsFileOldFileBe = TSUIDfileoldFile.replaceAll("202[0-9].{15}", "");
                		String sTsBe = INI.replaceAll("202[0-9].{15}", "");

                		if (tsFileOldFileBe.equals(sTsBe)) {

                		} else if (!tsFileOldFileBe.equals(sTsBe)) {
                			FileOutputStream fileOutStream = new FileOutputStream(sPgminiPath);
                			OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
                			BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
                			bufferedWriter.write(INI);
                			bufferedWriter.close();
                			chgCount ++;
                		}
                	}

                	File TSUCDfile = new File(sPgmchgPath);

                	if (!TSUCDfile.exists()) {

                		FileOutputStream fileOutStream = new FileOutputStream(sPgmchgPath);
                		OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
                		BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
                		bufferedWriter.write(CHG);
                		bufferedWriter.close();
                		chgCount ++;
                	} else {
                		FileInputStream fileInStream = new FileInputStream(sPgmchgPath);
                		InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
                		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                		int ch;
                		String TSUCDfileoldFile = "";

                		while((ch = bufferedReader.read()) != -1) {

                			TSUCDfileoldFile += (char)ch;
                		}

                		String tsFileOldFileBe = TSUCDfileoldFile.replaceAll("202[0-9].{15}", "");
                		String sTsBe = CHG.replaceAll("202[0-9].{15}", "");

                		if (tsFileOldFileBe.equals(sTsBe)) {

                		} else if (!tsFileOldFileBe.equals(sTsBe) && TSUCDfile.exists()) {
                			FileOutputStream fileOutStream = new FileOutputStream(sPgmchgPath);
                			OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
                			BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
                			bufferedWriter.write(CHG);
                			bufferedWriter.close();
                			chgCount ++;
                		}
                	}
                }

                if(!LCD.equals(null)&&!LID.equals(null)&&!tblsnps.equals("Y"))
                {
                	pstmCbs = connCbs.prepareStatement(cbsquery,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
                	pstmCbs.setString(1, "C" + tblid.substring(1));
                	cbsRs = pstmCbs.executeQuery();

                	cbsRs.last();

                	int cbsrowcount = 0;
                	cbsrowcount = cbsRs.getRow();

                	cbsRs.beforeFirst();

                	String arTblInfo = "";
                	String arFFDInfo = "";
                	int nFileLoc = 1;

                	while (cbsRs.next()) {
                		String cbsOwner = cbsRs.getString("OWNER");
                		String cbsTblNm = cbsRs.getString("TABLE_NAME");
                		String cbsTblCo = cbsRs.getString("TABLE_COMMENTS");
                		String cbsColumn = cbsRs.getString("COLUMN_NAME");
                		String cbsColumnCo = cbsRs.getString("COLUMN_COMMENTS");
                		String cbsType = cbsRs.getString("DATA_TYPE");
                		int cbsLength = cbsRs.getInt("DATA_LENGTH");
                		int cbsPrecision = cbsRs.getInt("DATA_PRECISION");
                		String cbsScale = cbsRs.getString("DATA_SCALE");
                		String cbsNull = cbsRs.getString("NULLABLE");
                		int cbsId = cbsRs.getInt("COLUMN_ID");
                		String cbsDefaultLen = cbsRs.getString("DEFAULT_LENGTH");
                		String cbsDataDefault = cbsRs.getString("DATA_DEFAULT").trim();
                		String cbsPk = cbsRs.getString("PK_YN");
                		
                		String tblDataType = "";
                		String ffdDataType = "";
                		int ffdDataPrec = 0;
                		int ffdDataScal = 0;
                		int ffdDataSize = 0;
                		String ffdformat = "";
                		String ffdNull = "0";

                		String arTblInfocol = "";
                		String arFFDInfocol = "";

                		if (cbsType.contains("TIMESTAMP")) {
                			cbsType = "TIMESTAMP";
                			tblDataType = "11";
                			ffdDataType = "3";
                			ffdDataPrec = 11;
                			ffdDataScal = 0;
                			ffdDataSize = 14;
                			ffdformat = "YYYYMMDDHH24MISSFF6";
                		} else if (cbsType.contains("VARCHAR")) {
                			tblDataType = "12";
                			ffdDataType = "3";
                			ffdDataPrec = cbsLength;
                			ffdDataScal = 0;
                			ffdDataSize = cbsLength;
                		} else if (cbsType.contains("NUMBER") && (cbsScale.equals(null) || cbsScale.equals("0") || cbsScale.equals(0))) {
                			tblDataType = "3";
                			ffdDataType = "2";
                			ffdDataPrec = cbsPrecision;
                			ffdDataScal = 0;
                			ffdDataSize = cbsPrecision + 1;
                		} else if (cbsType.contains("NUMBER")) {
                			tblDataType = "6";
                			ffdDataType = "2";
                			ffdDataPrec = cbsPrecision;
                			ffdDataScal = Integer.parseInt(cbsScale);
                			ffdDataSize = cbsPrecision + 2;
                		}

                		if (cbsNull.equals("N")) {
                			ffdNull = "1";
                		}

                		arTblInfocol = "+db_nm=db_nm\r"
                				+ "+table_nm=nm\r"
                				+ ".#nm=" + cbsColumn + "\"\r"
                				+ ".field_data_type_cd=" + tblDataType + "\"\r"
                				+ ".field_scale=" + ffdDataScal + "\"\r"
                				+ ".field_prec=" + ffdDataPrec + "\"\r"
                				+ ".key_yn=\"N\"\r"
                				+ ".desc1=\"\"\r"
                				+ ".ifiller1=\"0\"\r"
                				+ ".ifiller2=\"0\"\r"
                				+ ".ifiller3=\"0\"\r"
                				+ ".sfiller1=\"STGOWN\"\r"
                				+ ".sfiller2=NULL\r"
                				+ ".sfiller3=NULL\r"
                				+ "$+\r";

                		arTblInfo += arTblInfocol;

                		arFFDInfocol = "+ffd_id=ffd_id\r"
                				+ "#ffd_col_id=" + cbsId + "\"\r"
                				+ ".nm=" + cbsColumn + "\"\r"
                				+ ".col_data_type_cd=" + ffdDataType + "\"\r"
                				+ ".col_sep=\"|^,\"\r"
                				+ ".col_loc=" + cbsId + "\"\r"
                				+ ".col_size=" + ffdDataSize + "\"\r"
                				+ ".desc1=" + cbsColumnCo + "\"\r"
                				+ ".prec=" + ffdDataScal + "\"\r"
                				+ ".key_yn=\"N\"\r"
                				+ ".filler1=NULL\r"
                				+ ".filler2=NULL\r"
                				+ ".filler3=NULL\r"
                				+ ".sql_type_cd=" + tblDataType + "\"\r"
                				+ ".col_pos=" + nFileLoc + "\"\r"
                				+ ".table_field_type=" + cbsType + "\"\r"
                				+ ".table_field_name=" + cbsColumn + "\"\r"
                				+ ".table_precision=" + ffdDataPrec +"\"\r"
                				+ ".table_scale=" + ffdDataScal + "\"\r"
                				+ ".table_PK_nullability=" + ffdNull + "\"\r"
                				+ ".table_default_value=\"\"\r"
                				+ ".null_default_value=\"\"\r"
                				+ ".table_date_format=" + ffdformat + "\"\r"
                				+ "$+\r";
                				arFFDInfo += arFFDInfocol;
                				nFileLoc += ffdDataSize;
                	}

                	String schgPgmPath = "D:\\\\#PGM\\\\#작업\\\\java\\\\TS\\\\CHG\\\\TSP_" + LCD + ".ts";
                	String siniPgmPath = "D:\\\\#PGM\\\\#작업\\\\java\\\\TS\\\\CHG\\\\TSP_" + LID + ".ts";
                	String sSrciniPgmPath0010 = "D:\\\\#PGM\\\\#작업\\\\java\\\\INI\\\\" + sub + "\\\\" + LID + "_US_0010.sql";
                	String sSrciniPgmPath0020 = "D:\\\\#PGM\\\\#작업\\\\java\\\\INI\\\\" + sub + "\\\\" + LID + "_LD_0020.sql";
                	String sSrciniPgmPath0030 = "D:\\\\#PGM\\\\#작업\\\\java\\\\INI\\\\" + sub + "\\\\" + LID + "_US_0030.sql";
                	String sSrciniPgmPath0040 = "D:\\\\#PGM\\\\#작업\\\\java\\\\INI\\\\" + sub + "\\\\" + LID + "_US_0040.sql";
                	String sSrcchgPgmPath0010 = "D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\" + sub + "\\\\" + LCD + "_US_0010.sql";
                	String sSrcchgPgmPath0020 = "D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\" + sub + "\\\\" + LCD + "_LD_0020.sql";
                	String sSrcchgPgmPath0030 = "D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\" + sub + "\\\\" + LCD + "_US_0030.sql";
                	String sSrcchgPgmPath0040 = "D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\x\\" + sub + "\\\\" + LCD + "_US_0040.sql";

                	String sDesciniUS0010 = "Staging Truncate";
                	String sDesciniLD0020 = "Staging Load";
                	String sDesciniUS0030 = "SOR Truncate";
                	String sDesciniUS0040 = "SOR Insert";

                	String sDescchgUS0010 = "Staging Truncate";
                	String sDescchgLD0020 = "Staging Load";
                	String sDescchgUS0030 = "";
                	String sDescchgUS0040 = "SOR Insert";

                	if (loadchk.equals("초기적재형")) {
                		sDescchgUS0030 = "SOR Truncate";
                	} else {
                		sDescchgUS0030 = "SOR Delete";
                	}

                	String siniUS_0010 = "";
                	String siniLD_0020 = "";
                	String siniUS_0030 = "";
                	String siniUS_0040 = "";

                	String schgUS_0010 = "";
                	String schgLD_0020 = "";
                	String schgUS_0030 = "";
                	String schgUS_0040 = "";

                	File fileini0010 = new File(sSrciniPgmPath0010);
                	if (!fileini0010.exists()) {
                		System.out.println("[" + tblno + "]" + "[" + tblid + "]" + "[" + tblname + "]" + "[" + LID + "]" + " 파일이 없어!");
                	} else {
                		FileInputStream fileInStream = new FileInputStream(sSrciniPgmPath0010);
                		InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
                		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                		int ch;

                		while((ch = bufferedReader.read()) != -1) {
                			siniUS_0010 += (char)ch;
                		}
                	}

                	File fileini0020 = new File(sSrciniPgmPath0020);
                	if (!fileini0020.exists()) {
                		System.out.println("[" + tblno + "]" + "[" + tblid + "]" + "[" + tblname + "]" + "[" + LID + "]" + " 파일이 없어!");
                	} else {
                		FileInputStream fileInStream = new FileInputStream(sSrciniPgmPath0020);
                		InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
                		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                		int ch;

                		while((ch = bufferedReader.read()) != -1) {
                			siniLD_0020 += (char)ch;
                		}
                	}

                	File fileini0030 = new File(sSrciniPgmPath0030);
                	if (!fileini0030.exists()) {
                		System.out.println("[" + tblno + "]" + "[" + tblid + "]" + "[" + tblname + "]" + "[" + LID + "]" + " 파일이 없어!");
                	} else {
                		FileInputStream fileInStream = new FileInputStream(sSrciniPgmPath0030);
                		InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
                		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                		int ch;

                		while((ch = bufferedReader.read()) != -1) {
                			siniUS_0030 += (char)ch;
                		}
                	}

                	File fileini0040 = new File(sSrciniPgmPath0040);
                	if (!fileini0040.exists()) {
                		System.out.println("[" + tblno + "]" + "[" + tblid + "]" + "[" + tblname + "]" + "[" + LID + "]" + " 파일이 없어!");
                	} else {
                		FileInputStream fileInStream = new FileInputStream(sSrciniPgmPath0040);
                		InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
                		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                		int ch;

                		while((ch = bufferedReader.read()) != -1) {
                			siniUS_0040 += (char)ch;
                		}
                	}

                	File filechg0010 = new File(sSrcchgPgmPath0010);
                	if (!filechg0010.exists()) {
                		System.out.println("[" + tblno + "]" + "[" + tblid + "]" + "[" + tblname + "]" + "[" + LCD + "]" + " 파일이 없어!");
                	} else {
                		FileInputStream fileInStream = new FileInputStream(sSrcchgPgmPath0010);
                		InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
                		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                		int ch;

                		while((ch = bufferedReader.read()) != -1) {
                			schgUS_0010 += (char)ch;
                		}
                	}

                	File filechg0020 = new File(sSrcchgPgmPath0020);
                	if (!filechg0020.exists()) {
                		System.out.println("[" + tblno + "]" + "[" + tblid + "]" + "[" + tblname + "]" + "[" + LCD + "]" + " 파일이 없어!");
                	} else {
                		FileInputStream fileInStream = new FileInputStream(sSrcchgPgmPath0020);
                		InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
                		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                		int ch;

                		while((ch = bufferedReader.read()) != -1) {
                			schgLD_0020 += (char)ch;
                		}
                	}

                	File filechg0030 = new File(sSrcchgPgmPath0030);
                	if (!filechg0030.exists()) {
                		System.out.println("[" + tblno + "]" + "[" + tblid + "]" + "[" + tblname + "]" + "[" + LCD + "]" + " 파일이 없어!");
                	} else {
                		FileInputStream fileInStream = new FileInputStream(sSrcchgPgmPath0030);
                		InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
                		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                		int ch;

                		while((ch = bufferedReader.read()) != -1) {
                			schgUS_0030 += (char)ch;
                			}
                	}

                	File filechg0040 = new File(sSrcchgPgmPath0040);
                	if (!filechg0040.exists()) {
                		System.out.println("[" + tblno + "]" + "[" + tblid + "]" + "[" + tblname + "]" + "[" + LCD + "]" + " 파일이 없어!");
                	} else {
                		FileInputStream fileInStream = new FileInputStream(sSrcchgPgmPath0040);
                		InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
                		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                		int ch;

                		while((ch = bufferedReader.read()) != -1) {
                			schgUS_0040 += (char)ch;
                		}
                	}

                	String sTsINI ="file=tsscm1.2\r"
                			+ "/ffd_grp 1 adwsor /DBFFD/ADW\r"
                			+ "/prj_grp " + GRP_INI + " adwsor /ADW/" + loadType2 + "/" + sub + "\r"
                			+ "$global_table_info 1\r"
                			+ "#db_nm=\"SOR\"\r"
                			+ "!nm=" + tblid + "\"\r"
                			+ ".desc1=\"\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".up_dt=" + CR_DT +"\"\r"
                			+".owner_nm = \"STGOWN\"\r"
                			+ ".ifiller1=\"0\"\r"
                			+ ".ifiller2=\"0\"\r"
                			+ ".ifiller3=\"0\"\r"
                			+ ".sfiller1=NULL\r"
                			+ ".sfiller2=NULL\r"
                			+ ".sfiller3=NULL\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "/global_table_info_dtl db_nm=db_nm AND table_nm=nm\r"
                			+ "$global_talbe_info_dtl 4\r"
                			+ arTblInfo
                			+ "$$\r"
                			+ "$prj 0\r"
                			+ ".prj_id=\"1\"\r"
                			+ "*prj_grp_id=" + GRP_INI + "\"\r"
                			+ "!nm=" + LID + "\"\r"
                			+ ".desc11=" + LIDnm + "\"\r"
                			+ ".cur_ver_id=\"2\"\r"
                			+ ".owner_nm=\"adwsor\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".up_dt=" + CR_DT + "\"\r"
                			+ ".perm=\"774\"\r"
                			+ ".grp_nm=\"adwsor\"\r"
                			+ ".favorite_yn=\\N\"\r"
                			+ ".status_cd=\"0\"\r"
                			+ ".sms_id=\"0\"\r"
                			+ ".eamil_id=\"0\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$arrow_dtl 4\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"2\"\r"
                			+ ".sequence=\"1\"\r"
                			+ ".sequence2=\"1\"\r"
                			+ ".pos_x=\"82\"\r"
                			+ ".pos_y=\"249\"\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"2\"\r"
                			+ ".sequence=\"1\"\r"
                			+ ".sequence2=\"2\"\r"
                			+ ".pos_x=\"166\"\r"
                			+ ".pos_y=\"249\"\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"3\"\r"
                			+ ".sequence=\"2\"\r"
                			+ ".sequence2=\"1\"\r"
                			+ ".pos_x=\"202\"\r"
                			+ ".pos_y=\"249\"\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"3\"\r"
                			+ ".sequence=\"2\"\r"
                			+ ".sequence2=\"2\"\r"
                			+ ".pos_x=\"286\"\r"
                			+ ".pos_y=\"249\"\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"4\"\r"
                			+ ".sequence=\"3\"\r"
                			+ ".sequence2=\"1\"\r"
                			+ ".pos_x=\"322\"\r"
                			+ ".pos_y=\"250\"\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"4\"\r"
                			+ ".sequence=\"3\"\r"
                			+ ".sequence2=\"2\"\r"
                			+ ".pos_x=\"406\"\r"
                			+ ".pos_y=\"250\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$block 4\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"2\"\r"
                			+ ".nm=\"US_0010\"\r"
                			+ ".block_kind_cd=\"1\"\r"
                			+ ".block_status_cd=\"1\"\r"
                			+ ".parallel=\"1\"\r"
                			+ ".comp=\"N\"\r"
                			+ ".pos_x=\"30\"\r"
                			+ ".pos_y=\"230\"\r"
                			+ ".desc1=" + sDesciniUS0010 + "\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".dt_dt=" + CR_DT + "\"\r"
                			+ ".block_kind_before=\"0\"\r"
                			+ ".block_kind_after=\"0\"\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"3\"\r"
                			+ ".nm=\"LD_0020\"\r"
                			+ ".block_kind_cd=\"9\"\r"
                			+ ".block_status_cd=\"1\"\r"
                			+ ".parallel=\"1\"\r"
                			+ ".comp=\"N\"\r"
                			+ ".pos_x=\"150\"\r"
                			+ ".pos_y=\"230\"\r"
                			+ ".desc1=" + sDesciniLD0020 + "\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".dt_dt=" + CR_DT + "\"\r"
                			+ ".block_kind_before=\"0\"\r"
                			+ ".block_kind_after=\"0\"\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"4\"\r"
                			+ ".nm=\"US_0030\"\r"
                			+ ".block_kind_cd=\"11\"\r"
                			+ ".block_status_cd=\"1\"\r"
                			+ ".parallel=\"1\"\r"
                			+ ".comp=\"N\"\r"
                			+ ".pos_x=\"270\"\r"
                			+ ".pos_y=\"230\"\r"
                			+ ".desc1=" + sDesciniUS0030 + "\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".dt_dt=" + CR_DT + "\"\r"
                			+ ".block_kind_before=\"0\"\r"
                			+ ".block_kind_after=\"0\"\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"5\"\r"
                			+ ".nm=\"US_0040\"\r"
                			+ ".block_kind_cd=\"11\"\r"
                			+ ".block_status_cd=\"1\"\r"
                			+ ".parallel=\"1\"\r"
                			+ ".comp=\"N\"\r"
                			+ ".pos_x=\"390\"\r"
                			+ ".pos_y=\"230\"\r"
                			+ ".desc1=" + sDesciniUS0040 + "\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".dt_dt=" + CR_DT + "\"\r"
                			+ ".block_kind_before=\"0\"\r"
                			+ ".block_kind_after=\"0\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$dep 4\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"2\"\r"
                			+ ".sequence=\"1\"\r"
                			+ ".dep_type_cd=\"1\"\r"
                			+ ".target_block_id=\"3\"\r"
                			+ ".link_kind_cd=\"1\"\r"
                			+ ".link_type_cd=\"1\"\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"3\"\r"
                			+ ".sequence=\"2\"\r"
                			+ ".dep_type_cd=\"1\"\r"
                			+ ".target_block_id=\"4\"\r"
                			+ ".link_kind_cd=\"1\"\r"
                			+ ".link_type_cd=\"1\"\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"4\"\r"
                			+ ".sequence=\"3\"\r"
                			+ ".dep_type_cd=\"1\"\r"
                			+ ".target_block_id=\"5\"\r"
                			+ ".link_kind_cd=\"1\"\r"
                			+ ".link_type_cd=\"1\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$loading2 4\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"3\"\r"
                			+ ".db_nm=\"SOR\"\r"
                			+ ".db_type_cd=\"2\"\r"
                			+ ".native_dsn=\"\"\r"
                			+ ".user_db_nm=\"\"\r"
                			+ ".user_nm=\"\"\r"
                			+ ".passwd=\"\"\r"
                			+ ".log_file=\"$$[SOR_" + loadType2 + "_LOG]/" + LID + ".log\"\r"
                			+ ".bad_file=\"$$[SOR_" + loadType2 + "_BAD]/" + LID + ".bad\"\r"
                			+ ".data_file=\"$$[SOR_" + loadType2 + "_SAM]/" + sub+ "/" + tblid + ".dat\"\r"
                			+ ".target_table=\"\"\r"
                			+ ".options=\"DIRECT=TRUE ERRORS=0\"\r"
                			+ ".ffd_id=\"1\"\r"
                			+ ".date_format=\"YYYYMMDD\"\r"
                			+ ".connect_type=\"1\"\r"
                			+ ".ctl_file=" + siniLD_0020 + "\"\r"
                			+ ".table_ffd_id=\"1\"\r"
                			+ ".load_type_cd=\"1\"\r"
                			+ ".ifiller1=\"0\"\r"
                			+ ".ifiller2=\"0\"\r"
                			+ ".ifiller3=\"0\"\r"
                			+ ".sfiller1=\"\"\r"
                			+ ".sfiller2=\"\"\r"
                			+ ".sfiller3=\"YYYYMMDDHH24MISSFF6\"\r"
                			+ ".ifiller4=\"0\"\r"
                			+ ".ifiller5=\"0\"\r"
                			+ ".ifiller6=\"0\"\r"
                			+ ".ifiller7=\"0\"\r"
                			+ ".ifiller8=\"0\"\r"
                			+ ".ifiller9=\"0\"\r"
                			+ ".ifiller10=\"0\"\r"
                			+ ".sfiller4=\"\"\r"
                			+ ".sfiller5=\"\"\r"
                			+ ".sfiller6=\"\"\r"
                			+ ".sfiller7=\"\"\r"
                			+ ".sfiller8=\"\"\r"
                			+ ".sfiller9=\"0\"\r"
                			+ ".sfiller10=\"\"\r"
                			+ ".data_type_cd=\"1\"\r"
                			+ ".dfs_nm=\"LOCAL\"\r"
                			+ ".max_errors=\"100\"\r"
                			+ ".keep_null=\"0\"\r"
                			+ ".table_level_lock=\"0\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$procedure_block 4\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"2\"\r"
                			+ ".db_nm=\"SOR\"\r"
                			+ ".nm=\"US_0010\"\r"
                			+ ".desc1=" + sDesciniUS0010 + "\"\r"
                			+ ".sql_source=\"set serveroutput on]n"
                			+ ".Declare\r"
                			+ "      o_result int;\r"
                			+ "      o_errmsg varchar2(4000);\r"
                			+ "\r"
                			+ "BEGIN\r"
                			+ siniUS_0010 + "\r"
                			+ "     dbms_output.put_line(\u0027o_result : \u0027|| o_result);\r"
                			+ "     dbms_output.put_line(\u0027o_result : \u0027|| o_errmsg);\r"
                			+ ".END;\r"
                			+"\"\r"
                			+ ".sp_sql=\"1\"\r"
                			+ ".sp1=\"0\"\r"
                			+ ".sp2=\"1\"\r"
                			+ ".sp3=\"0\"\r"
                			+ ".sp4=\"0\"\r"
                			+ ".sp_text=" + siniUS_0010 + "\"\r"
                			+ ".sybase_native_dsn=\"\"\r"
                			+ ".sp_option=\"\"\r"
                			+ ".ifiller1=\"1\"\r"
                			+ ".ifiller2=\"0\"\r"
                			+ ".ifiller3=\"0\"\r"
                			+ ".sfiller1=\"\"\r"
                			+ ".sfiller2=\"\"\r"
                			+ ".sfiller3=NULL\r"
                			+ ".stdout_file=NULL\r"
                			+ ".stderr_file=NULL\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"4\"\r"
                			+ ".db_nm=\"SOR\"\r"
                			+ ".nm=\"US_0030\"\r"
                			+ ".desc1=" + sDesciniUS0030 + "\"\r"
                			+ ".sql_source=\"set serveroutput on]n"
                			+ ".Declare\r"
                			+ ".       o_result int;\r"
                			+ ".       o_errmsg varchar2(4000);\r"
                			+ ".       o_count int;\r"
                			+ "\r"
                			+ "BEGIN\r"
                			+ siniUS_0030 + "\r"
                			+ "     dbms_output.put_line(\u0027o_result : \u0027|| o_result);\r"
                			+ "     dbms_output.put_line(\u0027o_result : \u0027|| o_errmsg);\r"
                			+ "     dbms_output.put_line(\u0027o_count : \u0027|| o_count);\r"
                			+ ".END;\r"
                			+"\"\r"
                			+ ".sp_sql=\"1\"\r"
                			+ ".sp1=\"0\"\r"
                			+ ".sp2=\"1\"\r"
                			+ ".sp3=\"1\"\r"
                			+ ".sp4=\"0\"\r"
                			+ ".sp_text=" + siniUS_0030 + "\"\r"
                			+ ".sybase_native_dsn=\"\"\r"
                			+ ".sp_option=\"\"\r"
                			+ ".ifiller1=\"1\"\r"
                			+ ".ifiller2=\"0\"\r"
                			+ ".ifiller3=\"0\"\r"
                			+ ".sfiller1=\"\"\r"
                			+ ".sfiller2=\"\"\r"
                			+ ".sfiller3=NULL\r"
                			+ ".stdout_file=NULL\r"
                			+ ".stderr_file=NULL\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"5\"\r"
                			+ ".db_nm=\"SOR\"\r"
                			+ ".nm=\"US_0040\"\r"
                			+ ".desc1=" + sDesciniUS0040 + "\"\r"
                			+ ".sql_source=\"set serveroutput on]n"
                			+ ".Declare\r"
                			+ ".       o_result int;\r"
                			+ ".       o_errmsg varchar2(4000);\r"
                			+ ".       o_count int;\r"
                			+ "\r"
                			+ "BEGIN\r"
                			+ siniUS_0040 + "\r"
                			+ "     dbms_output.put_line(\u0027o_result : \u0027|| o_result);\r"
                			+ "     dbms_output.put_line(\u0027o_result : \u0027|| o_errmsg);\r"
                			+ "     dbms_output.put_line(\u0027o_count : \u0027|| o_count);\r"
                			+ ".END;\r"
                			+"\"\r"
                			+ ".sp_sql=\"1\"\r"
                			+ ".sp1=\"0\"\r"
                			+ ".sp2=\"1\"\r"
                			+ ".sp3=\"1\"\r"
                			+ ".sp4=\"0\"\r"
                			+ ".sp_text=" + siniUS_0040 + "\"\r"
                			+ ".sybase_native_dsn=\"\"\r"
                			+ ".sp_option=\"\"\r"
                			+ ".ifiller1=\"1\"\r"
                			+ ".ifiller2=\"0\"\r"
                			+ ".ifiller3=\"0\"\r"
                			+ ".sfiller1=\"\"\r"
                			+ ".sfiller2=\"\"\r"
                			+ ".sfiller3=NULL\r"
                			+ ".stdout_file=NULL\r"
                			+ ".stderr_file=NULL\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$prj_ber 4\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".ver_nm=\"\"\r"
                			+ ".desc1=\"<viewsize>1000;1000</viewsize>\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".up_dt=" + CR_DT + "\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$note_memo 4\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"1\"\r"
                			+ ".nm=\"프로젝트 정보\"\r"
                			+ ".block_kind_cd=\"13\"\r"
                			+ ".block_status_cd=\"1\"\r"
                			+ ".pos_x=\"787404\"\r"
                			+ ".pos_y=\"786610\"\r"
                			+ ".desc1=\"bsEllipse\r"
                			+ "1. 프로젝트 명 : " + LID + " - " + LIDnm + "\r"
                			+ "2. 변경이력\r"
                			+ "     일자                      작성자                 변경내용\r"
                			+ "     ---------------  -------------   -----------------------------------------------------------------------------------------------\r"
                			+ "     2020-09-09    ADW                   최초작성\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".up_dt=" + CR_DT + "\"\r"
                			+ ".block_kind_before=\"0\"\r"
                			+ ".block_kind_after=\"0\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$global_ffd 1\r"
                			+ "^ffd_id=\"1\"\r"
                			+ ".col_type_cd=\"2\"\r"
                			+ ".length=\"0\"\r"
                			+ ".num_col=" + cbsrowcount + "\"\r"
                			+ "!nm=" + tblid + "\"\r"
                			+ ".desc1=" + tblname + "\"\r"
                			+ ".cr_date=\"2020-09-09 17:50:44\"\r"
                			+ ".du_date=\"2020-09-09 18:19:57\"\r"
                			+ ".owner_nm=\"adwsor\"\r"
                			+ "*ffd_grp_id=\"1\"\r"
                			+ ".db_nm=\"SOR\"\r"
                			+ ".table_nm=" + tblid + "\"\r"
                			+ ".ffd_type=\"5\"\r"
                			+ ".perm=\"664\"\r"
                			+ ".grp_nm=\"adwsor\"\r"
                			+ ".ifiller1=\"1\"\r"
                			+ ".ifiller2=\"0\"\r"
                			+ ".ifiller3=\"0\"\r"
                			+ ".sfiller1=\"/DBFFD/ADW\"\r"
                			+ ".sfiller2=NULL\r"
                			+ ".sfiller3=\"STGOWN\"\r"
                			+ ".status_cd=\"0\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "/global_ffd_dtl ffd_id=ffd_id\r"
                			+ "$global_ffd_dtl 4\r"
                			+ arFFDInfo
                			+ "$$\r";


                	String sTsCHG ="file=tsscm1.2\r"
                			+ "/ffd_grp 1 adwsor /DBFFD/ADW\r"
                			+ "/prj_grp " + GRP_CHG + " adwsor /ADW/" + loadType + "/" + sub + "\r"
                			+ "$global_table_info 1\r"
                			+ "#db_nm=\"SOR\"\r"
                			+ "!nm=" + tblid + "\"\r"
                			+ ".desc1=\"\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".up_dt=" + CR_DT +"\"\r"
                			+".owner_nm = \"STGOWN\"\r"
                			+ ".ifiller1=\"0\"\r"
                			+ ".ifiller2=\"0\"\r"
                			+ ".ifiller3=\"0\"\r"
                			+ ".sfiller1=NULL\r"
                			+ ".sfiller2=NULL\r"
                			+ ".sfiller3=NULL\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "/global_table_info_dtl db_nm=db_nm AND table_nm=nm\r"
                			+ "$global_talbe_info_dtl 4\r"
                			+ arTblInfo
                			+ "$$\r"
                			+ "$prj 0\r"
                			+ ".prj_id=\"1\"\r"
                			+ "*prj_grp_id=" + GRP_CHG + "\"\r"
                			+ "!nm=" + LCD + "\"\r"
                			+ ".desc11=" + LCDnm + "\"\r"
                			+ ".cur_ver_id=\"2\"\r"
                			+ ".owner_nm=\"adwsor\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".up_dt=" + CR_DT + "\"\r"
                			+ ".perm=\"774\"\r"
                			+ ".grp_nm=\"adwsor\"\r"
                			+ ".favorite_yn=\\N\"\r"
                			+ ".status_cd=\"0\"\r"
                			+ ".sms_id=\"0\"\r"
                			+ ".eamil_id=\"0\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$arrow_dtl 4\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"2\"\r"
                			+ ".sequence=\"1\"\r"
                			+ ".sequence2=\"1\"\r"
                			+ ".pos_x=\"82\"\r"
                			+ ".pos_y=\"249\"\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"2\"\r"
                			+ ".sequence=\"1\"\r"
                			+ ".sequence2=\"2\"\r"
                			+ ".pos_x=\"166\"\r"
                			+ ".pos_y=\"249\"\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"3\"\r"
                			+ ".sequence=\"2\"\r"
                			+ ".sequence2=\"1\"\r"
                			+ ".pos_x=\"202\"\r"
                			+ ".pos_y=\"249\"\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"3\"\r"
                			+ ".sequence=\"2\"\r"
                			+ ".sequence2=\"2\"\r"
                			+ ".pos_x=\"286\"\r"
                			+ ".pos_y=\"249\"\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"4\"\r"
                			+ ".sequence=\"3\"\r"
                			+ ".sequence2=\"1\"\r"
                			+ ".pos_x=\"322\"\r"
                			+ ".pos_y=\"250\"\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"4\"\r"
                			+ ".sequence=\"3\"\r"
                			+ ".sequence2=\"2\"\r"
                			+ ".pos_x=\"406\"\r"
                			+ ".pos_y=\"250\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$block 4\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"2\"\r"
                			+ ".nm=\"US_0010\"\r"
                			+ ".block_kind_cd=\"1\"\r"
                			+ ".block_status_cd=\"1\"\r"
                			+ ".parallel=\"1\"\r"
                			+ ".comp=\"N\"\r"
                			+ ".pos_x=\"30\"\r"
                			+ ".pos_y=\"230\"\r"
                			+ ".desc1=" + sDescchgUS0010 + "\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".dt_dt=" + CR_DT + "\"\r"
                			+ ".block_kind_before=\"0\"\r"
                			+ ".block_kind_after=\"0\"\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"3\"\r"
                			+ ".nm=\"LD_0020\"\r"
                			+ ".block_kind_cd=\"9\"\r"
                			+ ".block_status_cd=\"1\"\r"
                			+ ".parallel=\"1\"\r"
                			+ ".comp=\"N\"\r"
                			+ ".pos_x=\"150\"\r"
                			+ ".pos_y=\"230\"\r"
                			+ ".desc1=" + sDescchgLD0020 + "\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".dt_dt=" + CR_DT + "\"\r"
                			+ ".block_kind_before=\"0\"\r"
                			+ ".block_kind_after=\"0\"\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"4\"\r"
                			+ ".nm=\"US_0030\"\r"
                			+ ".block_kind_cd=\"11\"\r"
                			+ ".block_status_cd=\"1\"\r"
                			+ ".parallel=\"1\"\r"
                			+ ".comp=\"N\"\r"
                			+ ".pos_x=\"270\"\r"
                			+ ".pos_y=\"230\"\r"
                			+ ".desc1=" + sDescchgUS0030 + "\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".dt_dt=" + CR_DT + "\"\r"
                			+ ".block_kind_before=\"0\"\r"
                			+ ".block_kind_after=\"0\"\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"5\"\r"
                			+ ".nm=\"US_0040\"\r"
                			+ ".block_kind_cd=\"11\"\r"
                			+ ".block_status_cd=\"1\"\r"
                			+ ".parallel=\"1\"\r"
                			+ ".comp=\"N\"\r"
                			+ ".pos_x=\"390\"\r"
                			+ ".pos_y=\"230\"\r"
                			+ ".desc1=" + sDescchgUS0040 + "\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".dt_dt=" + CR_DT + "\"\r"
                			+ ".block_kind_before=\"0\"\r"
                			+ ".block_kind_after=\"0\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$dep 4\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"2\"\r"
                			+ ".sequence=\"1\"\r"
                			+ ".dep_type_cd=\"1\"\r"
                			+ ".target_block_id=\"3\"\r"
                			+ ".link_kind_cd=\"1\"\r"
                			+ ".link_type_cd=\"1\"\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"3\"\r"
                			+ ".sequence=\"2\"\r"
                			+ ".dep_type_cd=\"1\"\r"
                			+ ".target_block_id=\"4\"\r"
                			+ ".link_kind_cd=\"1\"\r"
                			+ ".link_type_cd=\"1\"\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"4\"\r"
                			+ ".sequence=\"3\"\r"
                			+ ".dep_type_cd=\"1\"\r"
                			+ ".target_block_id=\"5\"\r"
                			+ ".link_kind_cd=\"1\"\r"
                			+ ".link_type_cd=\"1\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$loading2 4\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"3\"\r"
                			+ ".db_nm=\"SOR\"\r"
                			+ ".db_type_cd=\"2\"\r"
                			+ ".native_dsn=\"\"\r"
                			+ ".user_db_nm=\"\"\r"
                			+ ".user_nm=\"\"\r"
                			+ ".passwd=\"\"\r"
                			+ ".log_file=\"$$[SOR_" + loadType + "_LOG]/" + LCD + ".log\"\r"
                			+ ".bad_file=\"$$[SOR_" + loadType + "_BAD]/" + LCD + ".bad\"\r"
                			+ ".data_file=\"$$[SOR_" + loadType + "_SAM]/" + sub+ "/" + tblid + ".dat\"\r"
                			+ ".target_table=\"\"\r"
                			+ ".options=\"DIRECT=TRUE ERRORS=0\"\r"
                			+ ".ffd_id=\"1\"\r"
                			+ ".date_format=\"YYYYMMDD\"\r"
                			+ ".connect_type=\"1\"\r"
                			+ ".ctl_file=" + schgLD_0020 + "\"\r"
                			+ ".table_ffd_id=\"1\"\r"
                			+ ".load_type_cd=\"1\"\r"
                			+ ".ifiller1=\"0\"\r"
                			+ ".ifiller2=\"0\"\r"
                			+ ".ifiller3=\"0\"\r"
                			+ ".sfiller1=\"\"\r"
                			+ ".sfiller2=\"\"\r"
                			+ ".sfiller3=\"YYYYMMDDHH24MISSFF6\"\r"
                			+ ".ifiller4=\"0\"\r"
                			+ ".ifiller5=\"0\"\r"
                			+ ".ifiller6=\"0\"\r"
                			+ ".ifiller7=\"0\"\r"
                			+ ".ifiller8=\"0\"\r"
                			+ ".ifiller9=\"0\"\r"
                			+ ".ifiller10=\"0\"\r"
                			+ ".sfiller4=\"\"\r"
                			+ ".sfiller5=\"\"\r"
                			+ ".sfiller6=\"\"\r"
                			+ ".sfiller7=\"\"\r"
                			+ ".sfiller8=\"\"\r"
                			+ ".sfiller9=\"0|\"\r"
                			+ ".sfiller10=\"\"\r"
                			+ ".data_type_cd=\"1\"\r"
                			+ ".dfs_nm=\"LOCAL\"\r"
                			+ ".max_errors=\"100\"\r"
                			+ ".keep_null=\"0\"\r"
                			+ ".table_level_lock=\"0\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$procedure_block 4\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"2\"\r"
                			+ ".db_nm=\"SOR\"\r"
                			+ ".nm=\"US_0010\"\r"
                			+ ".desc1=" + sDescchgUS0010 + "\"\r"
                			+ ".sql_source=\"set serveroutput on]n"
                			+ ".Declare\r"
                			+ ".       o_result int;\r"
                			+ ".       o_errmsg varchar2(4000);\r"
                			+ "\r"
                			+ "BEGIN\r"
                			+ schgUS_0010 + "\r"
                			+ "     dbms_output.put_line(\u0027o_result : \u0027|| o_result);\r"
                			+ "     dbms_output.put_line(\u0027o_result : \u0027|| o_errmsg);\r"
                			+ ".END;\r"
                			+"\"\r"
                			+ ".sp_sql=\"1\"\r"
                			+ ".sp1=\"0\"\r"
                			+ ".sp2=\"1\"\r"
                			+ ".sp3=\"0\"\r"
                			+ ".sp4=\"0\"\r"
                			+ ".sp_text=" + schgUS_0010 + "\"\r"
                			+ ".sybase_native_dsn=\"\"\r"
                			+ ".sp_option=\"\"\r"
                			+ ".ifiller1=\"1\"\r"
                			+ ".ifiller2=\"0\"\r"
                			+ ".ifiller3=\"0\"\r"
                			+ ".sfiller1=\"\"\r"
                			+ ".sfiller2=\"\"\r"
                			+ ".sfiller3=NULL\r"
                			+ ".stdout_file=NULL\r"
                			+ ".stderr_file=NULL\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"4\"\r"
                			+ ".db_nm=\"SOR\"\r"
                			+ ".nm=\"US_0030\"\r"
                			+ ".desc1=" + sDescchgUS0030 + "\"\r"
                			+ ".sql_source=\"set serveroutput on]n"
                			+ ".Declare\r"
                			+ ".       o_result int;\r"
                			+ ".       o_errmsg varchar2(4000);\r"
                			+ ".       o_count int;\r"
                			+ "\r"
                			+ "BEGIN\r"
                			+ schgUS_0030 + "\r"
                			+ "     dbms_output.put_line(\u0027o_result : \u0027|| o_result);\r"
                			+ "     dbms_output.put_line(\u0027o_result : \u0027|| o_errmsg);\r"
                			+ "     dbms_output.put_line(\u0027o_count : \u0027|| o_count);\r"
                			+ ".END;\r"
                			+"\"\r"
                			+ ".sp_sql=\"1\"\r"
                			+ ".sp1=\"0\"\r"
                			+ ".sp2=\"1\"\r"
                			+ ".sp3=\"1\"\r"
                			+ ".sp4=\"0\"\r"
                			+ ".sp_text=" + schgUS_0030 + "\"\r"
                			+ ".sybase_native_dsn=\"\"\r"
                			+ ".sp_option=\"\"\r"
                			+ ".ifiller1=\"1\"\r"
                			+ ".ifiller2=\"0\"\r"
                			+ ".ifiller3=\"0\"\r"
                			+ ".sfiller1=\"\"\r"
                			+ ".sfiller2=\"\"\r"
                			+ ".sfiller3=NULL\r"
                			+ ".stdout_file=NULL\r"
                			+ ".stderr_file=NULL\r"
                			+ "$+\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"5\"\r"
                			+ ".db_nm=\"SOR\"\r"
                			+ ".nm=\"US_0040\"\r"
                			+ ".desc1=" + sDescchgUS0040 + "\"\r"
                			+ ".sql_source=\"set serveroutput on]n"
                			+ ".Declare\r"
                			+ ".       o_result int;\r"
                			+ ".       o_errmsg varchar2(4000);\r"
                			+ ".       o_count int;\"\r"
                			+ "\r"
                			+ "BEGIN\r"
                			+ schgUS_0040 + "\r"
                			+ "     dbms_output.put_line(\u0027o_result : \u0027|| o_result);\r"
                			+ "     dbms_output.put_line(\u0027o_result : \u0027|| o_errmsg);\r"
                			+ "     dbms_output.put_line(\u0027o_count : \u0027|| o_count);\r"
                			+ ".END;\r"
                			+"\"\r"
                			+ ".sp_sql=\"1\"\r"
                			+ ".sp1=\"0\"\r"
                			+ ".sp2=\"1\"\r"
                			+ ".sp3=\"1\"\r"
                			+ ".sp4=\"0\"\r"
                			+ ".sp_text=" + schgUS_0040 + "\"\r"
                			+ ".sybase_native_dsn=\"\"\r"
                			+ ".sp_option=\"\"\r"
                			+ ".ifiller1=\"1\"\r"
                			+ ".ifiller2=\"0\"\r"
                			+ ".ifiller3=\"0\"\r"
                			+ ".sfiller1=\"\"\r"
                			+ ".sfiller2=\"\"\r"
                			+ ".sfiller3=NULL\r"
                			+ ".stdout_file=NULL\r"
                			+ ".stderr_file=NULL\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$prj_ber 4\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".ver_nm=\"\"\r"
                			+ ".desc1=\"<viewsize>1000;1000</viewsize>\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".up_dt=" + CR_DT + "\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$note_memo 4\r"
                			+ ".prj_id=\"1\"\r"
                			+ ".prj_ver_id=\"1\"\r"
                			+ ".block_id=\"1\"\r"
                			+ ".nm=\"프로젝트 정보\"\r"
                			+ ".block_kind_cd=\"13\"\r"
                			+ ".block_status_cd=\"1\"\r"
                			+ ".pos_x=\"787404\"\r"
                			+ ".pos_y=\"786610\"\r"
                			+ ".desc1=\"bsEllipse\r"
                			+ "1. 프로젝트 명 : " + LCD + " - " + LCDnm + "\r"
                			+ "2. 변경이력\r"
                			+ "     일자                      작성자                 변경내용\r"
                			+ "     ---------------  -------------   -----------------------------------------------------------------------------------------------\r"
                			+ "     2020-09-09    ADW                   최초작성\"\r"
                			+ ".cr_dt=" + CR_DT + "\"\r"
                			+ ".up_dt=" + CR_DT + "\"\r"
                			+ ".block_kind_before=\"0\"\r"
                			+ ".block_kind_after=\"0\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "$global_ffd 1\r"
                			+ "^ffd_id=\"1\"\r"
                			+ ".col_type_cd=\"2\"\r"
                			+ ".length=\"0\"\r"
                			+ ".num_col=" + cbsrowcount + "\"\r"
                			+ "!nm=" + tblid + "\"\r"
                			+ ".desc1=" + tblname + "\"\r"
                			+ ".cr_date=\"2020-09-09 17:50:44\"\r"
                			+ ".du_date=\"2020-09-09 18:19:57\"\r"
                			+ ".owner_nm=\"adwsor\"\r"
                			+ "*ffd_grp_id=\"1\"\r"
                			+ ".db_nm=\"SOR\"\r"
                			+ ".table_nm=" + tblid + "\"\r"
                			+ ".ffd_type=\"5\"\r"
                			+ ".perm=\"664\"\r"
                			+ ".grp_nm=\"adwsor\"\r"
                			+ ".ifiller1=\"1\"\r"
                			+ ".ifiller2=\"0\"\r"
                			+ ".ifiller3=\"0\"\r"
                			+ ".sfiller1=\"/DBFFD/ADW\"\r"
                			+ ".sfiller2=NULL\r"
                			+ ".sfiller3=\"STGOWN\"\r"
                			+ ".status_cd=\"0\"\r"
                			+ "$+\r"
                			+ "$$\r"
                			+ "/global_ffd_dtl ffd_id=ffd_id\r"
                			+ "$global_ffd_dtl 4\r"
                			+ arFFDInfo
                			+ "$$\r"
                			+ "";


                	File TSLIDfile = new File(siniPgmPath);

                	if (!TSLIDfile.exists()) {

                		FileOutputStream fileOutStream = new FileOutputStream(siniPgmPath);
                		OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
                		BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
                		bufferedWriter.write(sTsINI);
                		bufferedWriter.close();
                		chgCount ++;
                	} else {
                		FileInputStream fileInStream = new FileInputStream(siniPgmPath);
                		InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
                		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                		int ch;
                		String TSLIDfileoldFile = "";

                		while((ch = bufferedReader.read()) != -1) {

                			TSLIDfileoldFile += (char)ch;
                		}

                		String tsFileOldFileBe = TSLIDfileoldFile.replaceAll("202[0-9].{15}", "");
                		String sTsBe = sTsINI.replaceAll("202[0-9].{15}", "");

                		if (tsFileOldFileBe.equals(sTsBe)) {

                		} else if (!tsFileOldFileBe.equals(sTsBe) && TSLIDfile.exists()) {
                			FileOutputStream fileOutStream = new FileOutputStream(siniPgmPath);
                			OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
                			BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
                			bufferedWriter.write(sTsINI);
                			bufferedWriter.close();
                			chgCount ++;
                		}
                	}

                	File TSLCDfile = new File(schgPgmPath);

                	if (!TSLCDfile.exists()) {

                		FileOutputStream fileOutStream = new FileOutputStream(schgPgmPath);
                		OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
                		BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
                		bufferedWriter.write(sTsCHG);
                		bufferedWriter.close();
                		chgCount ++;
                	} else {
                		FileInputStream fileInStream = new FileInputStream(schgPgmPath);
                		InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
                		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                		int ch;
                		String TSLCDfileoldFile = "";

                		while((ch = bufferedReader.read()) != -1) {

                			TSLCDfileoldFile += (char)ch;
                		}

                		String tsFileOldFileBe = TSLCDfileoldFile.replaceAll("202[0-9].{15}", "");
                		String sTsBe = sTsCHG.replaceAll("202[0-9].{15}", "");

                		if (tsFileOldFileBe.equals(sTsBe)) {

                		} else if (!tsFileOldFileBe.equals(sTsBe) && TSLCDfile.exists()) {
                			FileOutputStream fileOutStream = new FileOutputStream(schgPgmPath);
                			OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
                			BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
                			bufferedWriter.write(sTsCHG);
                			bufferedWriter.close();
                			chgCount ++;
                		}
                	}
                }if(chgCount==0){
                	System.out.println("[" + tblno + "]" + "[" + tblid + "]" + "[" + tblname + "]" + " [ts파일 변경없음]");
                	notChangeCount++;
                } else {
                	System.out.println("[" + tblno + "]" + "[" + tblid + "]" + "[" + tblname + "]" + " [ts파일 생성완료]");
                	allChangeCount++;
                }

			}

	int allTablecnt = notChangeCount + allChangeCount + ifTableCount + delTableCount;
	System.out.println("===============================================");
			System.out.println("총"+delTableCount+" 건 테이블 삭제]");
			System.out.println("총"+ifTableCount+" 건 테이블 I/F적재형]");
			System.out.println("총"+notChangeCount+"	건 테이블	ts 변경 없음]");
			System.out.println("총"+allChangeCount+"	건 테이블	ts 생성	작업 완료]");
			System.out.println("총"+allTablecnt+" 건 테이블 작업 완료]");
			System.out.println("===============================================");
	}catch(SQLException|IOException sqle){
		System.out.println("예외발생");
		sqle.printStackTrace();
		}finally{
			try{
				if(cbsRs!=null){
					cbsRs.close();
					}
				if(pstmCbs!=null){
					pstmCbs.close();
					}
				if(connCbs!=null){
					connCbs.close();
					}
				}catch(Exception e){
					throw new RuntimeException(e.getMessage());
				}
		}
        return"";
}
}
