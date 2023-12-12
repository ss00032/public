import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
public class ReadFile {
	public static void main(String[] args) throws IOException {
		
		FileInputStream fileInStream = new FileInputStream("C:\\신한 데이터댐\\01.업무\\04.외부데이터 테이블 정의서_20211103\\외부데이터\\새 폴더\\1_ECOS_KeyStatisticList.sql");
		InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
		
		String str;
		while ((str = bufferedReader.readLine()) != null) {
			System.out.println(str);
			}
		bufferedReader.close();
		}
}
