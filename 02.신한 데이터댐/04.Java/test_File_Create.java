import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.SQLException;

public class test_File_Create {
	public static void main(String args[]) {

		try {

			String owner = "SOROWN";
			String tblID = "ABC";
			String tblPartnm = tblID + "_PTR$$[1]";
			String LCDSNPfilePath0030 = "C:\\신한 데이터댐\\연습\\0030.sql";

			String sqlSnp0030Trc = "COMMIT;\r" + "\r" + "DECLARE\r"
					+ "          P_RTN                                          VARCHAR2(1000);\r" + "BEGIN\r"
					+ "           EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FROMAT = \\\\\"YYYYMMDD\\\\\"';\r"
					+ "           EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = \\\\\"YYYYMMDDHH24MISS\\\\\"';\r"
					+ "           EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';\r" + "\r"
					+ "           SP_ADW_STATS_GATHER (P_RTN, '" + owner + "', " + "'" + tblID + "', " + "'" + tblPartnm
					+ "');\r" + "\r" + "          COMMIT;\r" + "END;\r";

			File LCDSNPfile0030 = new File(LCDSNPfilePath0030);

			if (!LCDSNPfile0030.exists()) {

				FileOutputStream fileOutStream = new FileOutputStream(LCDSNPfilePath0030);
				OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
				BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
				bufferedWriter.write(sqlSnp0030Trc);
				bufferedWriter.close();
			} else {
				FileInputStream fileInStream = new FileInputStream(LCDSNPfilePath0030);
				InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
				BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

				int ch;
				String LCDSnpFile0030OldFile = "";

				while ((ch = bufferedReader.read()) != -1) {

					LCDSnpFile0030OldFile += (char) ch;
				}
				System.out.println(LCDSnpFile0030OldFile);

				if (LCDSnpFile0030OldFile.equals(sqlSnp0030Trc)) {

				} else if (!LCDSnpFile0030OldFile.equals(sqlSnp0030Trc) && LCDSNPfile0030.exists()) {
					FileOutputStream fileOutStream = new FileOutputStream(LCDSNPfilePath0030);
					OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
					BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
					bufferedWriter.write(sqlSnp0030Trc);
					bufferedWriter.close();
				}
			}

		} catch (IOException sqle) {
			System.out.println("예외발생");
			sqle.printStackTrace();
		} finally {
		}
		return;
	}
}