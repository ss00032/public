import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
public class ReadFile {
	public static void main(String[] args) throws IOException {
		
		FileInputStream fileInStream = new FileInputStream("C:\\���� �����ʹ�\\01.����\\04.�ܺε����� ���̺� ���Ǽ�_20211103\\�ܺε�����\\�� ����\\1_ECOS_KeyStatisticList.sql");
		InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
		
		String str;
		while ((str = bufferedReader.readLine()) != null) {
			System.out.println(str);
			}
		bufferedReader.close();
		}
}
