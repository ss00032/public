import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.util.Arrays;

public class Example {
	public static void main(String[] args) throws IOException {
		String dirPath = "C:\\���� �����ʹ�\\01.����\\04.�ܺε����� ���̺� ���Ǽ�_20211103\\�ܺε�����\\�� ���� (2)";
		
		showFilesInDIr(dirPath);
	}
	public static void showFilesInDIr(String dirPath) {
		try {
			File dir = new File(dirPath);
		    File files[] = dir.listFiles();
		    String Result = "";
		    String filename = "";
		    System.out.println("try�� ����� Ÿ�°ž�");
		    for (int i = 0; i < files.length; i++) {
		    	
		    	
		    	String TableKorean = "";
    			String TableEnglish = "";
    			String Extract = null;
		        File file = files[i];
		        filename = file.getName();
		        System.out.println("i="+i + "length="+files.length+"file="+file);
		        if (file.isDirectory()) {
		            showFilesInDIr(file.getPath());
		            continue;
		            //System.out.println("ddd="+file.getName());
		        } else {
		        	FileInputStream fileInStream = new FileInputStream(file);
		    		InputStreamReader inputStreamReader = new InputStreamReader(fileInStream, "UTF8");
		    		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
		    		
		    		String str;
		    		while ((str = bufferedReader.readLine()) != null) {
		    				
		    			if(str.contains("���̺��")) {
		    				if(str.contains("��Ʈ")) {
		    					String TableK = str.replace(") : ", "/");
			    				//System.out.println("��" + Repl);
			    				//System.out.println("�ù�" + Repl.length());
			    				String SubS = TableK.substring(TableK.indexOf("/")+1, TableK.length());
			    				TableKorean = SubS;
			    				//System.out.println("���̺��ѱ۸� : " + TableKorean);
		    				}
		    				if(str.contains("����")) {
		    					String TableE = str.replace(") : ", "/");
			    				//System.out.println("��" + Repl);
			    				//System.out.println("�ù�" + Repl.length());
			    				String SubS = TableE.substring(TableE.indexOf("/")+1, TableE.length());
			    				TableEnglish = SubS;
			    				//System.out.println("���̺����� : " + TableEnglish);
		    				}
		    			}
		    			if(str.contains("`")) {
		    				if(!str.contains("TABLE"))
		    				{
		    					if(!str.contains("KEY")) {
		    						String Replace = str.replace(")", ")/").replace("` ", "|").replace(" `", "");
			    					//String SubStr = str.replace(" `", "").replace("` ", "/").replace(") ", ")/").replace(" ", "");
			    					if(str.contains("datetime")) {
			    						Replace = str.replace("datetime", "datetime/").replace("` ", "|").replace(" `", "");
			    						//System.out.println(Replace);
			    					}
			    					if(str.contains("timestamp")) {
			    						Replace = str.replace("timestamp", "timestamp/").replace("` ", "|").replace(" `", "");
			    					}
				    				
				    				//System.out.println(Replace);
				    				String SubStr = Replace.substring(1,Replace.indexOf("/"));
				    				String [] Split = SubStr.split("`",1);
				    				for (String s : Split) {
				    					Extract = TableKorean + "|" + TableEnglish + "|" + s + "\n";
				    					//System.out.println("���� : " + Extract+ ":" + filename);
				    					//System.out.println("��� : " + Result);
				    					}
				    				Result += Extract;
				    				//System.out.println(Result);
		    					}
		    					
		    					//System.out.println(Result);
		    				}
		    				//System.out.println(Extract);
		    			}
		    			//System.out.println(Extract);
		    		}
		    		bufferedReader.close();
    				//System.out.println("ī��Ʈ : "+Count);
		    		System.out.println("��� : "+ i + Result);
		    		}
		        }
		    System.out.println("file :" + filename);
	        //System.out.println("��� : "+ i + Result);
	        String SavePath = "C:\\���� �����ʹ�\\01.����\\04.�ܺε����� ���̺� ���Ǽ�_20211103\\�ܺε�����\\����\\" + filename + ".sql";
	        FileOutputStream fileOutStream = new FileOutputStream(SavePath);
			OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
			BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
			bufferedWriter.write(Result);
			bufferedWriter.close();
		    //ExtractResult += Result + "\n";
		    //System.out.println("��� : " + ExtractResult);
		    //BufferedOutputStream allFileWriter = new BufferedOutputStream(new FileOutputStream(SavePath));
		    //allFileWriter.write(Result.getBytes());
		    //allFileWriter.close();
		    
		} catch (Exception e) {
			System.out.println("unkonwn error");
			e.printStackTrace();
		}
	    
	}
}
