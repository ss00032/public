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
		System.out.println("ó������");
	
		File path = new File("C:\\���� �����ʹ�\\01.����\\04.�ܺε����� ���̺� ���Ǽ�_20211103\\�ܺε�����\\�� ����\\���̺� ��Ű��.20211103\\274-324_SEOUL_���￭�������ͱ���");
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
			System.out.print("����");
		}
		
		System.out.print("ó���Ϸ�");
		
	}

}
