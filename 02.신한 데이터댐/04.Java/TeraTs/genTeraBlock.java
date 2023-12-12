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

import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;

public class genTeraBlock {

	public String Terablock() {
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

			String cbsquery = "SELECT  \r" + "                     A.OWNER\r" + "                     ,A.TABLE_NAME\r"
					+ "                     ,B.COMMENTS AS TABLE_COMMENTS\r"
					+ "                     ,C.COMMENTS AS COLUMN_COMMENTS\r" + "                     ,A.DATA_TYPE\r"
					+ "                     ,A.DATA_LENGTH\r" + "                     ,A.DATA_PRECISION\r"
					+ "                     ,A.DATA_SCALE\r" + "                     ,A.NULLABLE\r"
					+ "                     ,A.COLUMN_ID\r" + "                     ,A.DEFAULT_LENGTH\r"
					+ "                     ,TRIM(REPLACE(DBMS_XMLGEN.GETXMLTYPE('SELECT DATA_DEFAULT FROM ALL_TAB_COLS WHERE OWNER='''||A.OWNER||''' AND TABLE_NAME = '''||A.TABLE_NAME||''' AND COLUMN_NAME = '''||A.COLUMN_NAME||''' ').EXTRACT('//text()'),'&apos;','''')) AS DATA_DEFAULT\r"
					+ "                     ,(CASE WHEN D.COLUMN_NAME IS NULL THEN 'N' ELSE 'Y' END) AS PK_YN\r"
					+ "            FROM ALL_TAB_COLS A\r" + "            LEFT \r"
					+ "             JOIN ALL_TAB_COMMENTS B\r" + "                  ON B.OWNER = A.OWNER\r"
					+ "               AND B.TABLE_NAME = A.TABLE_NAME\r" + "              LEFT \r"
					+ "              JOIN ALL_COL_COMMENTS C\r" + "                 ON C.OWNER = A.OWNER\r"
					+ "              AND C.TABLE_NAME = A.TABLE_NAME\r"
					+ "               AND C.COLUMN_NAME = A.COLUMN_NAME\r" + "              LEFT \r"
					+ "              JOIN ALL_IND_COLUMNS D \r" + "                   ON D.TABLE_OWNER = A.OWNER\r"
					+ "                  AND D.TABLE_NAME = A.TABLE_NAME\r"
					+ "                  AND D.COLUMN_NAME = A.COLUMN_NAME\r"
					+ "                   AND (D.INDEX_NAME LIKE 'PK%' OR D.INDEX_NAME LIKE '%PK')\r"
					+ "               WHERE A.OWNER = 'CBSOWN'\r" + "                   AND A.TABLE_NAME = ?\r"
					+ "                   AND A.COLUMN_NAME NOT LIKE '%PSWD'\r"
					+ "                   AND A.HIDDEN_COLUMN = 'NO'\r"
					+ "                ORDER BY A.OWNER, A.TABLE_NAME, A.COLUMN_ID";
			String cbspartquery = "SELECT \r" + "                     TABLE_OWNER\r"
					+ "                    ,TABLE_NAME\r" + "                    ,PARTITION_NAME\r"
					+ "                     ,TRIM(REPLACE(DBMS_XMLGEN.GETXMLTYPE('SELECT HIGH_VALUE FROM ALL_TAB_PARTITIONS WHERE TABLE_OWNER='''||A.TABLE_OWNER||''' AND TABLE_NAME = '''||A.TABLE_NAME||''' AND PARTITION_NAME = '''||A.PARTITION_NAME||''' ').EXTRACT('//text()'),'&apos;','''')) AS HIGH_VALUE\r"
					+ "                    ,ROWNUM AS RN\r" + "        FROM ALL_TAB_PARTITIONS A\r"
					+ "      WHERE TABLE_OWNER = 'CBSOWN'\r" + "             AND TABLE_NAME = ?\r"
					+ "      ORDER BY TABLE_OWNER,TABLE_NAME,PARTITION_NAME";

			String sorquery = "SELECT  \r" + "                     A.OWNER\r" + "                     ,A.TABLE_NAME\r"
					+ "                     ,B.COMMENTS AS TABLE_COMMENTS\r"
					+ "                     ,C.COMMENTS AS COLUMN_COMMENTS\r" + "                     ,A.DATA_TYPE\r"
					+ "                     ,A.DATA_LENGTH\r" + "                     ,A.DATA_PRECISION\r"
					+ "                     ,A.DATA_SCALE\r" + "                     ,A.NULLABLE\r"
					+ "                     ,A.COLUMN_ID\r" + "                     ,A.DEFAULT_LENGTH\r"
					+ "                     ,TRIM(REPLACE(DBMS_XMLGEN.GETXMLTYPE('SELECT DATA_DEFAULT FROM ALL_TAB_COLS WHERE OWNER='''||A.OWNER||''' AND TABLE_NAME = '''||A.TABLE_NAME||''' AND COLUMN_NAME = '''||A.COLUMN_NAME||''' ').EXTRACT('//text()'),'&apos;','''')) AS DATA_DEFAULT\r"
					+ "                     ,(CASE WHEN D.COLUMN_NAME IS NULL THEN 'N' ELSE 'Y' END) AS PK_YN\r"
					+ "             ,MAX(LENGTH(A.COLUMN_NAME)) OVER() AS MAX_COL_LEN\r"
					+ "             ,MAX(LENGTH(D.COLUMN_NAME)) OVER() AS PK_MAX_COL_LEN\r"
					+ "            FROM ALL_TAB_COLS A\r" + "            LEFT \r"
					+ "             JOIN ALL_TAB_COMMENTS B\r" + "                  ON B.OWNER = A.OWNER\r"
					+ "               AND B.TABLE_NAME = A.TABLE_NAME\r" + "              LEFT \r"
					+ "              JOIN ALL_COL_COMMENTS C\r" + "                 ON C.OWNER = A.OWNER\r"
					+ "              AND C.TABLE_NAME = A.TABLE_NAME\r"
					+ "               AND C.COLUMN_NAME = A.COLUMN_NAME\r" + "              LEFT \r"
					+ "              JOIN ALL_IND_COLUMNS D \r" + "                   ON D.TABLE_OWNER = A.OWNER\r"
					+ "                  AND D.TABLE_NAME = A.TABLE_NAME\r"
					+ "                  AND D.COLUMN_NAME = A.COLUMN_NAME\r"
					+ "                   AND (D.INDEX_NAME LIKE 'PK%' OR D.INDEX_NAME LIKE '%PK')\r"
					+ "               WHERE A.OWNER = 'CBSOWN'\r" + "                   AND A.TABLE_NAME = ?\r"
					+ "                   AND A.HIDDEN_COLUMN = 'NO'\r"
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

			connCbs = cbsDBConnection.getConnection();
			connsor = sorDBConnection.getConnection();

			for (rowIndex = 2; rowIndex > rows; rowIndex++) {

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

				pstmsor = connsor.prepareStatement(sorquery);
				pstmsor.setString(1, tblID.getStringCellValue());

				pstmCbs = connCbs.prepareStatement(cbsquery);
				pstmCbs.setString(1, "C" + tblID.getStringCellValue().substring(1));

				int changeCount = 0;

				if (gbchk.equals("삭제")) {
					delTableCount++;
					continue;
				}

				String loadchk = tblloadchk.getStringCellValue().trim();

				if (loadchk.equals("I/F적재형")) {
					ifTableCount++;
					continue;
				}

				String sUSER = "";

				if (sub.equals("DOP") || sub.equals("DEC") || sub.equals("DPF")) {
					sUSER = "810257 조광희";
				}

				String UCDfilePath0010 = "D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\" + sub + "\\\\" + UCD + "_UN_0010.sql";
				String UIDfilePath0010 = "D:\\\\#PGM\\\\#작업\\\\java\\\\INI\\\\" + sub + "\\\\" + UID + "_UN_0010.sql";
				String LCDSNPfilePath0010 = "D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\" + sub + "\\\\" + LCD
						+ "_US_0010.sql";
				String LCDSNPfilePath0020 = "D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\" + sub + "\\\\" + LCD
						+ "_US_0020.sql";
				String LCDSNPfilePath0030 = "D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\" + sub + "\\\\" + LCD
						+ "_US_0030.sql";
				String LCDfilePath0010 = "D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\" + sub + "\\\\" + LCD + "_US_0010.sql";
				String LCDfilePath0020 = "D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\" + sub + "\\\\" + LCD + "_LD_0020.sql";
				String LCDfilePath0030 = "D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\" + sub + "\\\\" + LCD + "_US_0030.sql";
				String LCDfilePath0040 = "D:\\\\#PGM\\\\#작업\\\\java\\\\CHG\\\\" + sub + "\\\\" + LCD + "_US_0040.sql";
				String LIDfilePath0010 = "D:\\\\#PGM\\\\#작업\\\\java\\\\INI\\\\" + sub + "\\\\" + LID + "_US_0010.sql";
				String LIDfilePath0020 = "D:\\\\#PGM\\\\#작업\\\\java\\\\INI\\\\" + sub + "\\\\" + LID + "_LD_0020.sql";
				String LIDfilePath0030 = "D:\\\\#PGM\\\\#작업\\\\java\\\\INI\\\\" + sub + "\\\\" + LID + "_US_0030.sql";
				String LIDfilePath0040 = "D:\\\\#PGM\\\\#작업\\\\java\\\\INI\\\\" + sub + "\\\\" + LID + "_US_0040.sql";

				if (tblsnps.equals("Y")) {
					String tblOwner = "SOROWN";
					String tblPartnm = tblID + "_PTR$$[1]";

					String sqlSnp0010Trc = "COMMIT;\r" + "\r" + "DECLARE\r"
							+ "          P_RTN                                          VARCHAR2(1000);\r" + "BEGIN\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FROMAT = \\\\\"YYYYMMDD\\\\\"';\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = \\\\\"YYYYMMDDHH24MISS\\\\\"';\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';\r" + "\r"
							+ "            SP_ADW_TRUNCATE(P_RTN,'" + tblOwner + "','" + tblID + "','" + tblPartnm
							+ "');\r" + "\r" + "            IF P_RTN \u003c\u003e '0' THEN\r"
							+ "                          O_RESULT := 1;\r"
							+ "                          O_ERRMSG := 'TRUNCATE ERROR';\r" + "            END IF;\r"
							+ "\r" + "            COMMIT;\r" + "END;\r";

					File LCDSNPfile0010 = new File(LCDSNPfilePath0010);

					if (!LCDSNPfile0010.exists()) {

						FileOutputStream fileOutStream = new FileOutputStream(LCDSNPfilePath0010);
						OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
						BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
						bufferedWriter.write(sqlSnp0010Trc);
						bufferedWriter.close();
						changeCount++;
					} else {
						FileInputStream fileInStream = new FileInputStream(LCDSNPfilePath0010);
						InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
						BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

						int ch;
						String LCDSnpFile0010OldFile = "";

						while ((ch = bufferedReader.read()) != -1) {

							LCDSnpFile0010OldFile += (char) ch;
						}

						if (LCDSnpFile0010OldFile.equals(sqlSnp0010Trc)) {

						} else if (!LCDSnpFile0010OldFile.equals(sqlSnp0010Trc) && LCDSNPfile0010.exists()) {
							FileOutputStream fileOutStream = new FileOutputStream(LCDSNPfilePath0010);
							OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
							BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
							bufferedWriter.write(sqlSnp0010Trc);
							bufferedWriter.close();
							changeCount++;
						}
					}
				}

				if (tblsnps.equals("Y")) {
					String tblOwner = "SOROWN";
					String srcTbl = "D" + cbstbl.getStringCellValue().substring(1);

					sorRs = pstmsor.executeQuery();

					String columnlist = "";
					String colsqllist = "";

					while (sorRs.next()) {
						String sorowner = sorRs.getString("OWNER");
						String sortblnm = sorRs.getString("TABLE_NAME");
						String sortblco = sorRs.getString("TABLE_COMMENTS");
						String sorcolumn = sorRs.getString("COLUMN_NAME");
						String sorcolumnco = sorRs.getString("COLUMN_COMMENTS");
						String sortype = sorRs.getString("DATA_TYPE");
						String sorlength = sorRs.getString("DATA_LENGTH");
						String sorprecision = sorRs.getString("DATA_PRECISION");
						String sorscale = sorRs.getString("DATA_SCALE");
						String sornull = sorRs.getString("NULLABLE");
						int sorid = sorRs.getInt("COLUMN_ID");
						String sordefaultlen = sorRs.getString("DEFAULT_LENGTH");
						String sordefault = sorRs.getString("DATA_DEFAULT").trim();
						String sorpk = sorRs.getString("PK_YN");
						String sorcollen = sorRs.getString("MAX_COL_LEN");
						String sormaxcollen = sorRs.getString("PK_MAX_COL_LEN");

						String collist = null;
						if (sorid == 1) {
							collist = "                  (" + sorcolumn;
						} else {
							collist = "                   ," + sorcolumn;
						}
						columnlist += collist + "\r";

						String colsql = "                 ";
						if (sorid == 1) {
							colsql += " ";
						} else {
							colsql += ",";
						}
						if (sorcolumn.equals("SNPSH_BASE_DT")) {
							colsql += "'$$[1]'";
						} else if (sorcolumn.equals("DW_LDNG_DTTM")) {
							colsql += "TO_CHAR(SYSTIMESTAMP,'YYYYMMDDHH24MISSFF2')";
						} else {
							colsql += sorcolumn;
						}
						colsqllist += colsql + "\r";
					}

					String sqlSnp0020Trc = "COMMIT;\r" + "\r" + "DECLARE\r"
							+ "          P_RTN                                          VARCHAR2(1000);\r" + "BEGIN\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FROMAT = \\\\\"YYYYMMDD\\\\\"';\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = \\\\\"YYYYMMDDHH24MISS\\\\\"';\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';\r" + "\r"
							+ "            INSERT /* " + sUSER + " /ADW/CHG/" + sub + "/" + LCD + " - US_0020 */\r"
							+ "                             /*+ APPEND PARALLEL(4) */ \r"
							+ "                  INTO SOROWN." + tblID + "\r" + columnlist + "                      )\r"
							+ "SELECT /*+ FULL(T10) PARALLEL(4) */\r" + colsqllist + "       FROM SOROWN." + srcTbl
							+ " T10\r" + "       ;\r" + "\r" + "        O_COUNT := SQL%ROWCOUNT;\r"
							+ "        O_ERRMSG := SQLERRM;\r" + "\r" + "        COMMIT;\r" + "END;\r";

					File LCDSNPfile0020 = new File(LCDSNPfilePath0020);

					if (!LCDSNPfile0020.exists()) {

						FileOutputStream fileOutStream = new FileOutputStream(LCDSNPfilePath0020);
						OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
						BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
						bufferedWriter.write(sqlSnp0020Trc);
						bufferedWriter.close();
						changeCount++;
					} else {
						FileInputStream fileInStream = new FileInputStream(LCDSNPfilePath0020);
						InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
						BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

						int ch;
						String LCDSnpFile0020OldFile = "";

						while ((ch = bufferedReader.read()) != -1) {

							LCDSnpFile0020OldFile += (char) ch;
						}

						if (LCDSnpFile0020OldFile.equals(sqlSnp0020Trc)) {

						} else if (!LCDSnpFile0020OldFile.equals(sqlSnp0020Trc) && LCDSNPfile0020.exists()) {
							FileOutputStream fileOutStream = new FileOutputStream(LCDSNPfilePath0020);
							OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
							BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
							bufferedWriter.write(sqlSnp0020Trc);
							bufferedWriter.close();
							changeCount++;
						}
					}
				}

				if (tblsnps.equals("Y")) {
					String owner = "SOROWN";
					String tblPartnm = tblID + "_PTR$$[1]";

					String sqlSnp0030Trc = "COMMIT;\r" + "\r" + "DECLARE\r"
							+ "          P_RTN                                          VARCHAR2(1000);\r" + "BEGIN\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FROMAT = \\\\\"YYYYMMDD\\\\\"';\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = \\\\\"YYYYMMDDHH24MISS\\\\\"';\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';\r" + "\r"
							+ "           SP_ADW_STATS_GATHER (P_RTN, '" + owner + "', " + "'" + tblID + "', " + "'"
							+ tblPartnm + "');\r" + "\r" + "          COMMIT;\r" + "END;\r";

					File LCDSNPfile0030 = new File(LCDSNPfilePath0030);

					if (!LCDSNPfile0030.exists()) {

						FileOutputStream fileOutStream = new FileOutputStream(LCDSNPfilePath0030);
						OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
						BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
						bufferedWriter.write(sqlSnp0030Trc);
						bufferedWriter.close();
						changeCount++;
					} else {
						FileInputStream fileInStream = new FileInputStream(LCDSNPfilePath0030);
						InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
						BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

						int ch;
						String LCDSnpFile0030OldFile = "";

						while ((ch = bufferedReader.read()) != -1) {

							LCDSnpFile0030OldFile += (char) ch;
						}

						if (LCDSnpFile0030OldFile.equals(sqlSnp0030Trc)) {

						} else if (!LCDSnpFile0030OldFile.equals(sqlSnp0030Trc) && LCDSNPfile0030.exists()) {
							FileOutputStream fileOutStream = new FileOutputStream(LCDSNPfilePath0030);
							OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
							BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
							bufferedWriter.write(sqlSnp0030Trc);
							bufferedWriter.close();
							changeCount++;
						}
					}
				}

				if (!UCD.equals(null) && !tblsnps.equals("Y")) {
					cbsRs = pstmCbs.executeQuery();

					String UIDsql = "";
					String UCDsql = "";
					String tblOwner = "";
					String tblTblnm = "";

					while (cbsRs.next()) {
						String cbsowner = cbsRs.getString("OWNER");
						String cbstblnm = cbsRs.getString("TABLE_NAME");
						String cbstblco = cbsRs.getString("TABLE_COMMENTS");
						String cbscolumn = cbsRs.getString("COLUMN_NAME");
						String cbscolumnco = cbsRs.getString("COLUMN_COMMENTS");
						String cbstype = cbsRs.getString("DATA_TYPE");
						String cbslength = cbsRs.getString("DATA_LENGTH");
						String cbsprecision = cbsRs.getString("DATA_PRECISION");
						String cbsscale = cbsRs.getString("DATA_SCALE");
						String cbsnull = cbsRs.getString("NULLABLE");
						int cbsId = cbsRs.getInt("COLUMN_ID");
						String cbsdefaultLen = cbsRs.getString("DEFAULT_LENGTH");
						String cbsdefault = cbsRs.getString("DATA_DEFAULT").trim();
						String cbspk = cbsRs.getString("PK_YN");

						String U0010sql = "";
						tblOwner = cbsowner;
						tblTblnm = cbstblnm;

						if (cbsId == 1) {
							U0010sql += "             " + cbscolumn;
						} else if (cbscolumn.equals("RLNM_DVN_NO")) {
							U0010sql += "            ,TRIM(REPLACE(" + cbscolumn + ", '-', '') AS " + cbscolumn;
						}
						UCDsql += U0010sql + "\r";
						UIDsql += U0010sql + "\r";
					}

					String sqlUID0010Trc = "SELECT /* " + sUSER + " /ADW/INI/" + sub + "/" + UID + " - UN_0010 */\r"
							+ UIDsql + "    FROM " + tblOwner + "." + tblTblnm + "\r";

					File UIDfile0010 = new File(UIDfilePath0010);

					if (!UIDfile0010.exists()) {

						FileOutputStream fileOutStream = new FileOutputStream(UIDfilePath0010);
						OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
						BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
						bufferedWriter.write(sqlUID0010Trc);
						bufferedWriter.close();
						changeCount++;
					} else {
						FileInputStream fileInStream = new FileInputStream(UIDfilePath0010);
						InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
						BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

						int ch;
						String UIDFile0010OldFile = "";

						while ((ch = bufferedReader.read()) != -1) {

							UIDFile0010OldFile += (char) ch;
						}

						if (UIDFile0010OldFile.equals(sqlUID0010Trc)) {

						} else if (!UIDFile0010OldFile.equals(sqlUID0010Trc) && UIDfile0010.exists()) {
							FileOutputStream fileOutStream = new FileOutputStream(UIDfilePath0010);
							OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
							BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
							bufferedWriter.write(sqlUID0010Trc);
							bufferedWriter.close();
							changeCount++;
						}
					}

					String sqlUCD0010Trc = "";

					if (loadchk.equals("변경적재형") && !UCD.equals(null)) {
						sqlUCD0010Trc = "SELECT /* " + sUSER + " /ADW/CHG/" + sub + "/" + UCD + " - UN_0010 */\r"
								+ UCDsql + "    FROM " + tblOwner + "." + tblTblnm + "\r"
								+ " WHERE SYS_LAST_CHNG_DTTM <= '$$[1]'||'00000000'\r"
								+ "        OR FRMW_CHNG_TMST <= TO_TIMESTAMP('$$[1]'||'000000', 'YYYYMMDDHH24MISS')\r";
					} else if (loadchk.equals("초기적재형") && !UCD.equals(null)) {
						sqlUCD0010Trc = "SELECT /* " + sUSER + " /ADW/CHG/" + sub + "/" + UCD + " - UN_0010 */\r"
								+ UCDsql + "    FROM " + tblOwner + "." + tblTblnm + "\r";

						File UCDfile0010 = new File(UCDfilePath0010);

						if (!UCDfile0010.exists()) {

							FileOutputStream fileOutStream = new FileOutputStream(UCDfilePath0010);
							OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
							BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
							bufferedWriter.write(sqlUID0010Trc);
							bufferedWriter.close();
							changeCount++;
						} else {
							FileInputStream fileInStream = new FileInputStream(UCDfilePath0010);
							InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
							BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

							int ch;
							String UCDFile0010OldFile = "";

							while ((ch = bufferedReader.read()) != -1) {

								UCDFile0010OldFile += (char) ch;
							}

							if (UCDFile0010OldFile.equals(sqlUID0010Trc)) {

							} else if (!UCDFile0010OldFile.equals(sqlUID0010Trc) && UCDfile0010.exists()) {
								FileOutputStream fileOutStream = new FileOutputStream(UCDfilePath0010);
								OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
								BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
								bufferedWriter.write(sqlUID0010Trc);
								bufferedWriter.close();
								changeCount++;
							}
						}
					}
				}

				if (!LCD.equals(null) && !tblsnps.equals("Y")) {

					String tblOwner = "STGOWN";
					String tblPartnm = "";

					String sqlLD0010Trc = "COMMIT;\r" + "\r" + "DECLARE\r"
							+ "          P_RTN                                          VARCHAR2(1000);\r" + "BEGIN\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FROMAT = \\\\\"YYYYMMDD\\\\\"';\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = \\\\\"YYYYMMDDHH24MISS\\\\\"';\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';\r" + "\r"
							+ "           SP_ADW_TRUNCATE(P_RTN, '" + tblOwner + "', " + "'" + tblID + "', " + "'"
							+ tblPartnm + "');\r" + "\r" + "           IF P_RTN \u003c\u003e '0' THEN\r"
							+ "                   O_RESULT := 1;\r"
							+ "                   O_ERRMSG := 'TRUNCATE ERROR';\r" + "             END IF;\r" + "\r"
							+ "           COMMIT;\r" + "END;\r";

					File LIDfile0010 = new File(LIDfilePath0010);

					if (!LIDfile0010.exists()) {

						FileOutputStream fileOutStream = new FileOutputStream(LIDfilePath0010);
						OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
						BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
						bufferedWriter.write(sqlLD0010Trc);
						bufferedWriter.close();
						changeCount++;
					} else {
						FileInputStream fileInStream = new FileInputStream(LIDfilePath0010);
						InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
						BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

						int ch;
						String LIDFile0010OldFile = "";

						while ((ch = bufferedReader.read()) != -1) {

							LIDFile0010OldFile += (char) ch;
						}

						if (LIDFile0010OldFile.equals(sqlLD0010Trc)) {

						} else if (!LIDFile0010OldFile.equals(sqlLD0010Trc) && LIDfile0010.exists()) {
							FileOutputStream fileOutStream = new FileOutputStream(LIDfilePath0010);
							OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
							BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
							bufferedWriter.write(sqlLD0010Trc);
							bufferedWriter.close();
							changeCount++;
						}
					}

					File LCDfile0010 = new File(LCDfilePath0010);

					if (!LCDfile0010.exists()) {

						FileOutputStream fileOutStream = new FileOutputStream(LCDfilePath0010);
						OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
						BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
						bufferedWriter.write(sqlLD0010Trc);
						bufferedWriter.close();
						changeCount++;
					} else {
						FileInputStream fileInStream = new FileInputStream(LCDfilePath0010);
						InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
						BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

						int ch;
						String LCDFile0010OldFile = "";

						while ((ch = bufferedReader.read()) != -1) {

							LCDFile0010OldFile += (char) ch;
						}

						if (LCDFile0010OldFile.equals(sqlLD0010Trc)) {

						} else if (!LCDFile0010OldFile.equals(sqlLD0010Trc) && LCDfile0010.exists()) {
							FileOutputStream fileOutStream = new FileOutputStream(LCDfilePath0010);
							OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
							BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
							bufferedWriter.write(sqlLD0010Trc);
							bufferedWriter.close();
							changeCount++;
						}
					}
				}

				if (!LCD.equals(null) && !tblsnps.equals("Y")) {
					cbsRs = pstmCbs.executeQuery();

					String tblOwner = "STGOWN";
					String colInfos = "";

					while (cbsRs.next()) {
						String cbsowner = cbsRs.getString("OWNER");
						String cbstblnm = cbsRs.getString("TABLE_NAME");
						String cbstblco = cbsRs.getString("TABLE_COMMENTS");
						String cbscolumn = cbsRs.getString("COLUMN_NAME");
						String cbscolumnco = cbsRs.getString("COLUMN_COMMENTS");
						String cbstype = cbsRs.getString("DATA_TYPE");
						int cbslength = cbsRs.getInt("DATA_LENGTH");
						String cbsprecision = cbsRs.getString("DATA_PRECISION");
						String cbsscale = cbsRs.getString("DATA_SCALE");
						String cbsnull = cbsRs.getString("NULLABLE");
						int cbsId = cbsRs.getInt("COLUMN_ID");
						String cbsdefaultLen = cbsRs.getString("DEFAULT_LENGTH");
						String cbsdefault = cbsRs.getString("DATA_DEFAULT").trim();
						String cbspk = cbsRs.getString("PK_YN");

						String colInfo = "";
						String colType = "";

						if (cbsId == 1) {
							colInfo += "                " + rPad.padRight(cbscolumn, 32, ' ');
						} else {
							colInfo += "              ," + rPad.padRight(cbscolumn, 32, ' ');
						}

						if (cbstype.contains("TIMESTAMP")) {
							colType += "TIMESTAMP \\\\\"YYYYMMDDHH24MISSFF6\\\\\"";
						} else if (cbstype.contains("VARCHAR")) {
							colType = "CHAR";
							if (cbslength < 250) {
								colType += "(" + cbslength + ")";
							} else if (cbstype.contains("NUMBER") && cbsscale == null) {
								colType = "INTEGER EXTERNAL";
							} else if (cbstype.contains("NUMBER")) {
								colType = "DECIMAL EXTERNAL";
							}
							colInfos += colInfo + colType + "\r";
						}
					}

					String sqlLID0020Trc = "LOAD DATA\r" + "INFILE '$$[SOR_INI_SAM]/" + sub + "/" + tblID
							+ ".dat' \\\\\"STR '|^,\\\\\'\\\\\"\r" + "APPEND\r" + "INTO TABLE " + tblOwner + "." + tblID
							+ "\r" + "FIELDS TERMINATED BY '|^,'\r" + "TRAILING NULLCOLS\r" + "(\r" + colInfos + ")\r";
					String sqlLCD0020Trc = "LOAD DATA\r" + "INFILE '$$[SOR_CHG_SAM]/" + sub + "/" + tblID
							+ ".dat' \\\\\"STR '|^,\\\\\'\\\\\"\r" + "APPEND\r" + "INTO TABLE " + tblOwner + "." + tblID
							+ "\r" + "FIELDS TERMINATED BY '|^,'\r" + "TRAILING NULLCOLS\r" + "(\r" + colInfos + ")\r";

					File LIDfile0020 = new File(LIDfilePath0020);

					if (!LIDfile0020.exists()) {

						FileOutputStream fileOutStream = new FileOutputStream(LIDfilePath0020);
						OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
						BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
						bufferedWriter.write(sqlLID0020Trc);
						bufferedWriter.close();
						changeCount++;
					} else {
						FileInputStream fileInStream = new FileInputStream(LIDfilePath0020);
						InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
						BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

						int ch;
						String LIDFile0020OldFile = "";

						while ((ch = bufferedReader.read()) != -1) {

							LIDFile0020OldFile += (char) ch;
						}

						if (LIDFile0020OldFile.equals(sqlLID0020Trc)) {

						} else if (!LIDFile0020OldFile.equals(sqlLID0020Trc) && LIDfile0020.exists()) {
							FileOutputStream fileOutStream = new FileOutputStream(LIDfilePath0020);
							OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
							BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
							bufferedWriter.write(sqlLID0020Trc);
							bufferedWriter.close();
							changeCount++;
						}
					}

					File LCDfile0020 = new File(LCDfilePath0020);

					if (!LCDfile0020.exists()) {

						FileOutputStream fileOutStream = new FileOutputStream(LCDfilePath0020);
						OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
						BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
						bufferedWriter.write(sqlLCD0020Trc);
						bufferedWriter.close();
						changeCount++;
					} else {
						FileInputStream fileInStream = new FileInputStream(LCDfilePath0020);
						InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
						BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

						int ch;
						String LCDFile0020OldFile = "";

						while ((ch = bufferedReader.read()) != -1) {

							LCDFile0020OldFile += (char) ch;
						}

						if (LCDFile0020OldFile.equals(sqlLCD0020Trc)) {

						} else if (!LCDFile0020OldFile.equals(sqlLCD0020Trc) && LCDfile0020.exists()) {
							FileOutputStream fileOutStream = new FileOutputStream(LCDfilePath0020);
							OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
							BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
							bufferedWriter.write(sqlLCD0020Trc);
							bufferedWriter.close();
							changeCount++;
						}
					}
				}

				if (!LID.equals(null) && !tblsnps.equals("Y")) {

					String tblOwner = "SOROWN";
					String Partnm = "";
					String sqlLID0030Trc = "";

					if (!snpschk.equals("SNPSH_BASE_DT") && !snpschk.equals(null)) {
						String sqlPartTrc = "COMMIT;\r" + "\r" + "DECLARE\r"
								+ "      P_RTN              VARCHAR2(1000);\r" + "      V_OWNER      VARCHAR2(20) := '"
								+ tblOwner + "';\r" + "      V_TABLE         VARCHAR2(20) := '" + tblID + "';\r"
								+ "      V_PART_YN   VARCHAR2(1);\r" + "      V_CNT               NUMBER(10);\r"
								+ "BEGIN\r"
								+ "           EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FROMAT = \\\\\"YYYYMMDD\\\\\"';\r"
								+ "           EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = \\\\\"YYYYMMDDHH24MISS\\\\\"';\r"
								+ "           EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';\r" + "\r"
								+ "    SELECT \r"
								+ "         NVL(MAX(CASE WHEN PARTITIONED = 'YES' THEN 'Y' ELSE 'N' END), 'X') AS PART_YN\r"
								+ "        INTO V_PART_YN        \r" + "        FROM ALL_TABLES\r"
								+ "      WHERE OWNER = V_OWNER\r" + "           AND TABLE_NAME = V_TABLE\r"
								+ "        ;\r" + "\r"
								+ "          DBMS_OUTPUT.PUT_LINE(V_OWNER ||'.'|| V_TABLE || ' PART_YN : ['||V_PART_YN||']');\r"
								+ "          \r" + "          IF V_PART_YN = 'N' THEN\r"
								+ "                  SP_ADW_TRUNCATE(P_RTN,V_OWNER,V_TABLE,'');\r" + "\r"
								+ "          IF P_RTN \u003c\u003e '0' THEN =\r" + "               O_RESULT := 1;\r"
								+ "               O_ERRMSG := 'TRUNCATE ERROR';\r" + "          END IF;\r"
								+ "      ELSIF V_PART_YN = 'Y' THEN\r" + "               FOR PTR IN\r"
								+ "             (\r" + "                      SELECT \r"
								+ "                                  TABLE_OWNER\r"
								+ "                                  ,TABLE_NAME \r"
								+ "                                 ,PARTITION_NAME\r"
								+ "                            FROM ALL_TAB_PARTITIONS\r"
								+ "                       WHERE TABLE_OWNER = V_OWNER\r"
								+ "                           AND TABLE_NAME = V_TABLE\r" + "                  )\r"
								+ "                LOOP\r"
								+ "                          EXECUTE IMMEDIATE 'SELECT COUNT(1) FROM ' || PTR.TABLE_OWNER || '.' || PTR.TABLE_NAME || ' PARTITION('||PTR.PARTITION_NAME||')' INTO V_CNT;\r"
								+ "\r" + "                 IF V_CNT \u003e 0 THEN\r"
								+ "                           DBMS_OUTPUT.PUT_LINE(PTR.PARTITION_NAME|| ' Partition Data Count : '||V_CNT);\r"
								+ "                 \r" + "                            IF P_RTN \u003c\u003e '0' THEN\r"
								+ "                                      O_RESULT := 1;\r"
								+ "                                      O_ERRMSG := 'PARTITION TRUNCATE ERROR';\r"
								+ "                                      EXIT;\r"
								+ "                           END IF;\r" + "                 END IF;\r"
								+ "        END LOOP;\r" + "      ELSE\r" + "                O_RESULT := 1;\r"
								+ "                 O_ERRMSG := 'TABLE NOT EXIST ERROR';\r" + "        END IF;\r" + "\r"
								+ "           COMMIT;\r" + "END;\r";

						sqlLID0030Trc += sqlPartTrc + "\r";

						if (loadchk.equals("초기적재형")) {
							File LIDfile0030 = new File(LIDfilePath0030);

							if (!LIDfile0030.exists()) {

								FileOutputStream fileOutStream = new FileOutputStream(LIDfilePath0030);
								OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
								BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
								bufferedWriter.write(sqlLID0030Trc);
								bufferedWriter.close();
								changeCount++;
							} else {
								FileInputStream fileInStream = new FileInputStream(LIDfilePath0030);
								InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
								BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

								int ch;
								String LIDFile0030OldFile = "";

								while ((ch = bufferedReader.read()) != -1) {

									LIDFile0030OldFile += (char) ch;
								}

								if (LIDFile0030OldFile.equals(sqlLID0030Trc)) {

								} else if (!LIDFile0030OldFile.equals(sqlLID0030Trc) && LIDfile0030.exists()) {
									FileOutputStream fileOutStream = new FileOutputStream(LIDfilePath0030);
									OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream,
											"UTF8");
									BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
									bufferedWriter.write(sqlLID0030Trc);
									bufferedWriter.close();
									changeCount++;
								}
							}

							File LCDfile0030 = new File(LCDfilePath0030);

							if (!LCDfile0030.exists() && loadchk.equals("초기적재형")) {

								FileOutputStream fileOutStream = new FileOutputStream(LCDfilePath0030);
								OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
								BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
								bufferedWriter.write(sqlLID0030Trc);
								bufferedWriter.close();
								changeCount++;
							} else if (loadchk.equals("초기적재형")) {
								FileInputStream fileInStream = new FileInputStream(LCDfilePath0030);
								InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
								BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

								int ch;
								String LCDFile0030OldFile = "";

								while ((ch = bufferedReader.read()) != -1) {

									LCDFile0030OldFile += (char) ch;
								}

								if (LCDFile0030OldFile.equals(sqlLID0030Trc) && loadchk.equals("초기적재형")) {

								} else if (!LCDFile0030OldFile.equals(sqlLID0030Trc) && LCDfile0030.exists()
										&& loadchk.equals("초기적재형")) {
									FileOutputStream fileOutStream = new FileOutputStream(LCDfilePath0030);
									OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream,
											"UTF8");
									BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
									bufferedWriter.write(sqlLID0030Trc);
									bufferedWriter.close();
									changeCount++;
								}
							}
						}
					}
				}

				if (!LCD.equals(null) && !tblsnps.equals("Y") && loadchk.equals("변경적재형")) {
					sorRs = pstmsor.executeQuery();

					String tblOwner = "SOROWN";
					String Partnm = "";

					String PKchk = "";

					while (sorRs.next()) {
						String sorowner = sorRs.getString("OWNER");
						String sortblnm = sorRs.getString("TABLE_NAME");
						String sortblco = sorRs.getString("TABLE_COMMENTS");
						String sorcolumn = sorRs.getString("COLUMN_NAME");
						String sorcolumnco = sorRs.getString("COLUMN_COMMENTS");
						String sortype = sorRs.getString("DATA_TYPE");
						String sorlength = sorRs.getString("DATA_LENGTH");
						String sorprecision = sorRs.getString("DATA_PRECISION");
						String sorscale = sorRs.getString("DATA_SCALE");
						String sornull = sorRs.getString("NULLABLE");
						int sorid = sorRs.getInt("COLUMN_ID");
						String sordefaultlen = sorRs.getString("DEFAULT_LENGTH");
						String sordefault = sorRs.getString("DATA_DEFAULT").trim();
						String sorpk = sorRs.getString("PK_YN");
						String sorcollen = sorRs.getString("MAX_COL_LEN");
						int sormaxcollen = sorRs.getInt("PK_MAX_COL_LEN");

						String PK = "";

						if (sorpk.equals("Y")) {
							if (sorid == 1) {
								PK += "                         WHERE T11."
										+ rPad.padRight(sorcolumn, sormaxcollen, ' ') + " = T10." + sorcolumn + "\r";
							} else {
								PK += "                               AND T11."
										+ rPad.padRight(sorcolumn, sormaxcollen, ' ') + " = T10." + sorcolumn + "\r";
							}
						}
						PKchk += PK;
					}

					String sqlLCD0030Trc = "COMMIT;\r" + "\r" + "DECLARE\r"
							+ "      P_RTN              VARCHAR2(1000);\r" + "BEGIN\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FROMAT = \\\\\"YYYYMMDD\\\\\"';\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = \\\\\"YYYYMMDDHH24MISS\\\\\"';\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';\r" + "\r"
							+ "          DELETE /* " + sUSER + "  /ADW/CHG/" + sub + "/" + LCD + " - US_0030 */\r"
							+ "                            /*+ USE_HASH(T10 T11) PARALLEL(4) PQ_DISTRIBUTE(T10 HASH HASH) PQ_DISTRIBUTE(T11 HASH HASH) */ \r"
							+ "                FROM SOROWN." + tblID + " T10\r" + "             WHERE EXISTS (\r"
							+ "                                                   SELECT 'X'\r"
							+ "                                                         FROM STGOWN." + tblID + " T11\r"
							+ PKchk + "                                                )\r" + "                ;\r"
							+ "\r" + "            O_COUNT := SQL%ROWCOUNT;\r" + "            O_ERRMSG := SQLERRM;\r"
							+ "\r" + "           COMMIT;\r" + "END;\r";

					File LCDfile0030 = new File(LCDfilePath0030);

					if (!LCDfile0030.exists()) {

						FileOutputStream fileOutStream = new FileOutputStream(LCDfilePath0030);
						OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
						BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
						bufferedWriter.write(sqlLCD0030Trc);
						bufferedWriter.close();
						changeCount++;
					} else {
						FileInputStream fileInStream = new FileInputStream(LCDfilePath0030);
						InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
						BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

						int ch;
						String LCDFile0030OldFile = "";

						while ((ch = bufferedReader.read()) != -1) {

							LCDFile0030OldFile += (char) ch;
						}

						if (LCDFile0030OldFile.equals(sqlLCD0030Trc)) {

						} else if (!LCDFile0030OldFile.equals(sqlLCD0030Trc) && LCDfile0030.exists()) {
							FileOutputStream fileOutStream = new FileOutputStream(LCDfilePath0030);
							OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
							BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
							bufferedWriter.write(sqlLCD0030Trc);
							bufferedWriter.close();
							changeCount++;
						}
					}
				}

				if (!LCD.equals(null) && !tblsnps.equals("Y")) {
					sorRs = pstmsor.executeQuery();

					String tblOwner = "SOROWN";
					String Insertsql = "";
					String Selectsql = "";

					while (sorRs.next()) {
						String sorowner = sorRs.getString("OWNER");
						String sortblnm = sorRs.getString("TABLE_NAME");
						String sortblco = sorRs.getString("TABLE_COMMENTS");
						String sorcolumn = sorRs.getString("COLUMN_NAME");
						String sorcolumnco = sorRs.getString("COLUMN_COMMENTS");
						String sortype = sorRs.getString("DATA_TYPE");
						String sorlength = sorRs.getString("DATA_LENGTH");
						String sorprecision = sorRs.getString("DATA_PRECISION");
						String sorscale = sorRs.getString("DATA_SCALE");
						String sornull = sorRs.getString("NULLABLE");
						int sorid = sorRs.getInt("COLUMN_ID");
						String sordefaultlen = sorRs.getString("DEFAULT_LENGTH");
						String sordefault = sorRs.getString("DATA_DEFAULT").trim();
						String sorpk = sorRs.getString("PK_YN");
						String sorcollen = sorRs.getString("MAX_COL_LEN");
						String sormaxcollen = sorRs.getString("PK_MAX_COL_LEN");

						String LD0040sql = "";
						String Insert = "";
						if (sorid == 1) {
							LD0040sql = "                (" + sorcolumn;
						} else {
							LD0040sql = "                ," + sorcolumn;
						}
						Insert += LD0040sql + "\r";

						String LDsql = "                ";
						if (sorid == 1) {
							LDsql += " ";
						} else {
							LDsql += ",";
						}
						if (sorcolumn.equals("DW_LDNG_DTTM")) {
							LDsql += "TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISSFF2')";
						} else {
							LDsql += sorcolumn;
						}
						Selectsql += LDsql + "\r";
					}

					String sqlLID0040Trc = "COMMIT;\r" + "\r" + "DECLARE\r"
							+ "      P_RTN              VARCHAR2(1000);\r" + "BEGIN\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FROMAT = \\\\\"YYYYMMDD\\\\\"';\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = \\\\\"YYYYMMDDHH24MISS\\\\\"';\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';\r" + "\r"
							+ "           INSERT /* " + sUSER + " /ADW/INI/" + sub + "/" + LID + " - US_0040 */\r"
							+ "                            /* APPEND PARALLEL(4) */ \r"
							+ "                 INTO SOROWN." + tblID + "\r" + Insertsql
							+ "                           )\r" + "           SELECT /*+ FULL(T10) PARALLEL(4) */\r"
							+ Selectsql + "                FROM STGOWN." + tblID + " T10\r" + "              ;\r" + "\r"
							+ "            O_COUNT := SQL%ROWCOUNT;\r" + "            O_ERRMSG := SQLERRM;\r" + "\r"
							+ "           COMMIT;\r" + "END;\r";

					String sqlLCD0040Trc = "COMMIT;\r" + "\r" + "DECLARE\r"
							+ "      P_RTN              VARCHAR2(1000);\r" + "BEGIN\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FROMAT = \\\\\"YYYYMMDD\\\\\"';\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = \\\\\"YYYYMMDDHH24MISS\\\\\"';\r"
							+ "           EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';\r" + "\r"
							+ "           INSERT /* " + sUSER + " /ADW/CHG/" + sub + "/" + LCD + " - US_0040 */\r"
							+ "                            /* APPEND PARALLEL(4) */ \r"
							+ "                 INTO SOROWN." + tblID + "\r" + Insertsql
							+ "                           )\r" + "           SELECT /*+ FULL(T10) PARALLEL(4) */\r"
							+ Selectsql + "                FROM STGOWN." + tblID + " T10\r" + "              ;\r" + "\r"
							+ "            O_COUNT := SQL%ROWCOUNT;\r" + "            O_ERRMSG := SQLERRM;\r" + "\r"
							+ "           COMMIT;\r" + "END;\r";

					File LIDfile0040 = new File(LIDfilePath0040);

					if (!LIDfile0040.exists()) {

						FileOutputStream fileOutStream = new FileOutputStream(LIDfilePath0040);
						OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
						BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
						bufferedWriter.write(sqlLID0040Trc);
						bufferedWriter.close();
						changeCount++;
					} else {
						FileInputStream fileInStream = new FileInputStream(LIDfilePath0040);
						InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
						BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

						int ch;
						String LIDFile0040OldFile = "";

						while ((ch = bufferedReader.read()) != -1) {

							LIDFile0040OldFile += (char) ch;
						}

						if (LIDFile0040OldFile.equals(sqlLID0040Trc)) {

						} else if (!LIDFile0040OldFile.equals(sqlLID0040Trc) && LIDfile0040.exists()) {
							FileOutputStream fileOutStream = new FileOutputStream(LIDfilePath0040);
							OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
							BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
							bufferedWriter.write(sqlLID0040Trc);
							bufferedWriter.close();
							changeCount++;
						}
					}

					File LCDfile0040 = new File(LCDfilePath0040);

					if (!LCDfile0040.exists()) {

						FileOutputStream fileOutStream = new FileOutputStream(LCDfilePath0040);
						OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
						BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
						bufferedWriter.write(sqlLCD0040Trc);
						bufferedWriter.close();
						changeCount++;
					} else {
						FileInputStream fileInStream = new FileInputStream(LCDfilePath0040);
						InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
						BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

						int ch;
						String LCDFile0040OldFile = "";

						while ((ch = bufferedReader.read()) != -1) {

							LCDFile0040OldFile += (char) ch;
						}

						if (LCDFile0040OldFile.equals(sqlLCD0040Trc)) {

						} else if (!LCDFile0040OldFile.equals(sqlLCD0040Trc) && LCDfile0040.exists()) {
							FileOutputStream fileOutStream = new FileOutputStream(LCDfilePath0040);
							OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
							BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
							bufferedWriter.write(sqlLCD0040Trc);
							bufferedWriter.close();
							changeCount++;
						}
					}
				}

				if (changeCount == 0) {
					System.out.println("[" + tblno + "]" + "[" + tblID + "]" + "[" + tblname + "]" + " [Block 변경없음]");
					notChangeCount++;
				} else {
					System.out.println("[" + tblno + "]" + "[" + tblID + "]" + "[" + tblname + "]" + " [Block "
							+ changeCount + " 개 생성완료 ]");
					allChangeCount++;
				}
				pstmsor.close();
				pstmCbs.close();
			}
			int allTablecnt = notChangeCount + allChangeCount + ifTableCount + delTableCount;
			System.out.println("===============================================");
			System.out.println("총 " + delTableCount + " 건 테이블 삭제 ]");
			System.out.println("총 " + ifTableCount + " 건 테이블 I/F적재형 ]");
			System.out.println("총 " + notChangeCount + " 건 테이블 Block 변경 없음 ]");
			System.out.println("총 " + allChangeCount + " 건 테이블 Block 생성 작업 완료 ]");
			System.out.println("총 " + allTablecnt + " 건 테이블 작업 완료 ]");
			System.out.println("===============================================");
		} catch (SQLException | IOException sqle) {
			System.out.println("예외발생");
			sqle.printStackTrace();
		} finally {
			try {
				if (cbsRs != null) {
					cbsRs.close();
				}
				if (pstmCbs != null) {
					pstmCbs.close();
				}
				if (connCbs != null) {
					connCbs.close();
				}
			} catch (Exception e) {
				throw new RuntimeException(e.getMessage());
			}
		}
		return "";
	}
}