import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;

public class Incoding_extract {
	public static void main(String[] args) {
		FileInputStream FileInputStream = null;
		Reader reader = null;
		StringBuffer stringBuffer = new StringBuffer();
		Writer writer = null;
		
		int intRead = 0;
		System.out.println("처리시작");
	
		File path = new File("C:\\신한 데이터댐\\01.업무\\04.외부데이터 테이블 정의서_20211103\\외부데이터\\새 폴더\\테이블 스키마.20211103\\274-324_SEOUL_서울열린데이터광장");
		File[] list = path.listFiles();
		
		try {
			for(int i=0; i<=list.length -1; i++) {
				FileInputStream = new FileInputStream(list[i]);
				Charset inputCharset = Charset.forName("KSC5601");
				InputStreamReader isr = new InputStreamReader(FileInputStream, inputCharset);
				
				reader = new BufferedReader(isr);
				
				while((intRead = reader.read()) > -1) {
					stringBuffer.append((char)intRead);
				}
				reader.close();
				
				FileOutputStream fos = new FileOutputStream(list[i]);
				writer = new OutputStreamWriter(fos, "utf-8");
				writer.write("\uFEFF");
				writer.write(stringBuffer.toString());
				stringBuffer.setLength(0);
				writer.close();
			}
		} catch(IOException e) {
			System.out.print(e);
			System.out.print("실패");
		}
		
		System.out.print("처리완료");
		
	}

}
