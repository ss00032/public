import java.io.BufferedOutputStream;
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
import java.sql.SQLException;

import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class CBSConnection {
	public String CBSDDL() {
		
		Connection connCbs = null;
        PreparedStatement pstmCbs = null;
        PreparedStatement pstmCbsPart = null;
        ResultSet cbsRs = null;
        ResultSet cbsPartRs = null;

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
                                                        + "                     ,TRIM(REPLACE(DBMS_XMLGEN.GETXMLTYPE('SELECT DATA_DEFAULT FROM ALL_TAB_COLS WHERE OWNER='''||A.OWNER||''' AND TABLE_NAME = '''||A.TABLE_NAME||''' AND COLUMN_NAME = '''||A.COLUMN_NAME||''' ').EXTRACT('//text()'),'&apos;','''')) AS DATA_DEFAULT\r"
                                                        + "                     ,(CASE WHEN D.COLUMN_NAME IS NULL THEN 'N' ELSE 'Y' END) AS PK_YN\r"
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
                                                        + "                   AND (D.INDEX_NAME LIKE 'PK%' OR D.INDEX_NAME LIKE '%PK')\r"
                                                        + "               WHERE A.OWNER = 'CBSOWN'\r"
                                                        + "                   AND A.TABLE_NAME = ?\r"
                                                        + "                   AND A.COLUMN_NAME NOT LIKE '%PSWD'\r"
                                                        + "                   AND A.HIDDEN_COLUMN = 'NO'\r"
                                                        + "                ORDER BY A.OWNER, A.TABLE_NAME, A.COLUMN_ID";
                                            String cbsPartquery = "SELECT \r"
                                                        + "                     TABLE_OWNER\r"
                                                        + "                    ,TABLE_NAME\r"
                                                        + "                    ,PARTITION_NAME\r"
                                                        + "                     ,TRIM(REPLACE(DBMS_XMLGEN.GETXMLTYPE('SELECT HIGH_VALUE FROM ALL_TAB_PARTITIONS WHERE TABLE_OWNER='''||A.TABLE_OWNER||''' AND TABLE_NAME = '''||A.TABLE_NAME||''' AND PARTITION_NAME = '''||A.PARTITION_NAME||''' ').EXTRACT('//text()'),'&apos;','''')) AS HIGH_VALUE\r"
                                                        + "                    ,ROWNUM AS RN\r"
                                                        + "        FROM ALL_TAB_PARTITIONS A\r"
                                                        + "      WHERE TABLE_OWNER = 'CBSOWN'\r"
                                                        + "             AND TABLE_NAME = ?\r"
                                                        + "      ORDER BY TABLE_OWNER,TABLE_NAME,PARTITION_NAME";
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

                                          String allCbsResult = "";
                                          String chgCbsResult = "";
                                          String allFilePath = "전체 DDL 저장 경로";
                                          String chgFilePath = "변경 DDL 저장 경로";

                                         for (rowIndex = 2; rowIndex \u003c rows; rowIndex++) {

                                                        String tblOwner = "STGOWN";

                                                        XSSFRow row = sheet.getRow(rowIndex);
                                                        XSSFCell tblNo = row.getCell(0);
                                                        XSSFCell tblId = row.getCell(5);
                                                        XSSFCell tblNm = row.getCell(6);
                                                        XSSFCell tblGb = row.getCell(7);
                                                        XSSFCell tblPart = row.getCell(16);
                                                        XSSFCell tblloadchk = row.getCell(17);

                                                        String snpsChk = tblPart.getStringCellValue().trim();
                                                        String gbChk = tblGb.getStringCellValue().trim();
                                                        String tblName = tblNm.getStringCellValue();

                                                        int changeCount = 0;

                                                        if (snpsChk.equals("SNPSH_BASE_DT")) {

                                                                      snpTableCount++;
                                                                      continue;
                                                        }

                                                        if (gbChk.equals("삭제")) {
                                                                      delTableCount++;
                                                                      continue;
                                                        }

                                                        String loadchk = tblloadchk.getStringCellValue().trim();

                                                        if (loadchk.equals("I/F적재형")) {
                                                                      ifTableCount++;
                                                                      continue;
                                                        }

                                                        String filePath = "D:\\\\#PGM\\\\#작업\\\\java\\\\CBS\\\\STG_DDL\\\\"+tblId+".sql";

                                                        pstmCbs = connCbs.prepareStatement(cbsquery);
                                                        pstmCbs.setString(1, "C" + tblId.getStringCellValue().substring(1));
                                                        cbsRs = pstmCbs.executeQuery();

                                                        String cbsResult = null;
                                                        String colInfos = "";
                                                        String partYn = "";
                                                        String colnmCos = "";

                                                        while (cbsRs.next()) {
                                                                      String cbsOwner = cbsRs.getString("OWNER");
                                                                      String cbsTblNm = cbsRs.getString("TABLE_NAME");
                                                                      String cbsTblCo = cbsRs.getString("TABLE_COMMENTS");
                                                                      String cbsColumn = cbsRs.getString("COLUMN_NAME");
                                                                      String cbsColumnCo = cbsRs.getString("COLUMN_COMMENTS");
                                                                      String cbsType = cbsRs.getString("DATA_TYPE");
                                                                      String cbsLength = cbsRs.getString("DATA_LENGTH");
                                                                      String cbsPrecision = cbsRs.getString("DATA_PRECISION");
                                                                      String cbsScale = cbsRs.getString("DATA_SCALE");
                                                                      String cbsNull = cbsRs.getString("NULLABLE");
                                                                      int cbsId = cbsRs.getInt("COLUMN_ID");
                                                                      String cbsDefaultLen = cbsRs.getString("DEFAULT_LENGTH");
                                                                      String cbsDataDefault = cbsRs.getString("DATA_DEFAULT").trim();
                                                                      String cbsPk = cbsRs.getString("PK_YN");

                                                                      String colType = "";
                                                                      String colDef = "";
                                                                      String colNull = "";
                                                                      String colInfo = "      ";
                                                                      String colNmco = "";

                                                                      if (cbsType.contains("TIMESTAMP")) {
                                                                                    colType = cbsType;
                                                                      } else if (cbsType.contains("VARCHAR")) {
                                                                                    colType = cbsType + "(" + cbsLength + ")";
                                                                      } else if ( cbsType.contains("NUMBER") && cbsScale == null) {
                                                                                    colType = cbsType + "(" + cbsPrecision + ")";
                                                                      } else if (cbsType.contains("NUMBER")) {
                                                                                    colType = cbsType + "(" + cbsPrecision + "," + cbsScale + ")";
                                                                      }

                                                                      if (cbsDataDefault.length() != 0) {
                                                                    	  colDef = "DEFAULT " + cbsDataDefault;
                                                                      }

                                                                      if (cbsNull.equals("Y")) {
                                                                                    colNull += "    NULL";
                                                                      } else {
                                                                                    colNull += "NOT NULL";
                                                                      }

                                                                      if (cbsId == 1) {
                                                                                    colInfo += " ";
                                                                      } else {
                                                                                    colInfo += ",";
                                                                      }

                                                                      colInfo += rPad.padRight(cbsColumn, 30, ' ') + rPad.padRight(colType, 18, ' ') + rPad.padRight(colDef, 28, ' ') + colNull;
                                                                      colInfos += colInfo + "\r";
                                                                      
                                                                      colNmco = "COMMENT ON COLUMN " + tblOwner + "." + tblId + "." + rPad.padRight(cbsColumn, 30, ' ') + " IS  '" + cbsColumnCo + "';";
                                                                      colnmCos += colNmco + "\r";
                                                        }

                                                        if (snpsChk.length() != 0 && !snpsChk.equals("SNPSH_BASE_DT")) { 

pstmCbsPart = connCbs.prepareStatement(cbsPartquery,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
pstmCbsPart.setString(1,"C" + tblId.getStringCellValue().substring(1));
cbsPartRs = pstmCbsPart.executeQuery();

cbsPartRs.last();
int cbsPartrowCount = 0;
cbsPartrowCount = cbsPartRs.getRow();

if (cbsPartrowCount != 0 ) {
partYn = "SEGMENT CREATION IMMEDIATE\r"
+ "PCTFREE 0\r"
+ "STORAGE\r"
+ "(\r"
+ "INITIAL 1M\r"
+ "MAXEXTENTS 2147483645\r"
+ "PCTINCREASE 0\r"
+ ")\r"
+ "TABLESPACE CSTGST01";
}
cbsPartRs.beforeFirst();
}


cbsResult = "/**"+ tblId + " : " + tblName + "**/\r"
+ "\r"
+ "BEGIN EXECUTE IMMEDIATE 'DROP TABLE " + tblOwner + "." + tblId + " CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;/"
+ "\r"
+ "CREATE TABLE " + tblOwner + "." + tblId + "\r"
+ "(\r"
+ colInfos
+ ")\r"
+ partYn + "\r"
+ ";\r"
+ "\r"
+ "\r"
+ "GRANT SELECT ON " + tblOwner + "." + tblId + " TO RL_SOR_SEL;\r"
+ "GRANT INSERT, UPDATE, DELETE, SELECT ON " + tblOwner + "." + tblId + " TO RL_SOR_ALL;\r"
+ "\r"
+ "\r"
+ "COMMENT ON TABLE " + tblOwner + "." + tblId + " IS '" + tblName + "';\r"
+ "\r"
+ colnmCos + "\r"
+ "COMMIT;\r"
+ "\r"
+ "\r"
+ "\r";


allCbsResult += cbsResult + "\r" + "\r";


File file1 = new File(filePath);
if (!file1.exists()) {

	FileOutputStream fileOutStream = new FileOutputStream(filePath);
	OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
	BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
	bufferedWriter.write(cbsResult);
	chgCbsResult += cbsResult + "\r" + "\r";
	bufferedWriter.close();
	changeCount ++;
} else {
	FileInputStream fileInStream = new FileInputStream(filePath);
	InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
	BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

int ch;
String oldFile = "";

while((ch = bufferedReader.read()) != -1) {

oldFile += (char)ch;
}

if (oldFile.equals(cbsResult)) {

} else if (!oldFile.equals(cbsResult) && file1.exists()) {
FileOutputStream fileOutStream = new FileOutputStream(filePath);
OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
bufferedWriter.write(cbsResult);
chgCbsResult += cbsResult + "\r" + "\r";
bufferedWriter.close();
changeCount ++;
}
}
if (changeCount == 0) {
System.out.println("[" + tblNo + "]" + "[" + tblId + "]" + "[" + tblName + "]"  + " [DDL 변경없음]");
notChangeCount++;
} else {
System.out.println("[" + tblNo + "]" + "[" + tblId + "]" + "[" + tblName + "]"  + " [DDL 생성완료]");
allChangeCount++;
}

}

int allTableCount = allChangeCount + notChangeCount + delTableCount + ifTableCount + snpTableCount;
int crtnTableCount = allChangeCount + notChangeCount;

BufferedOutputStream allFileWriter = new BufferedOutputStream(new FileOutputStream(allFilePath));
allFileWriter.write(allCbsResult.getBytes());
allFileWriter.close();

System.out.println("=================================================");
System.out.println("[전체 테이블 " + allTableCount + " 건 테이블 중 총 " + crtnTableCount + " 건 DDL이. 생성되었습니다.]");

BufferedOutputStream chgFileWriter = new BufferedOutputStream(new FileOutputStream(chgFilePath));
chgFileWriter.write(chgCbsResult.getBytes());
chgFileWriter.close();
System.out.println("[변경된 DDL이 " + allChangeCount + " 건 생성되었습니다.]");
System.out.println("=================================================");

workBook.close();

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
}catch (Exception e){
throw new RuntimeException(e.getMessage());
}
}
return "";
}
}




