import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List; 
public class main { 
	public static void main(String[] args) { 
		List<String> fileLst = new ArrayList<String> (); 
		scanDir("C:\\���� �����ʹ�\\01.����\\04.�ܺε����� ���̺� ���Ǽ�_20211103\\�ܺε�����\\���̺� ��Ű��.20211103", fileLst);
		
		try {
			String SavePath = "C:\\���� �����ʹ�\\01.����\\04.�ܺε����� ���̺� ���Ǽ�_20211103\\�ܺε�����\\����\\����.sql";
			String TableKorean = "";
			String TableEnglish = "";
			String Extract = null;
			String Result = "";
			
			for(String fullPath : fileLst) { 
				System.out.println(fullPath); 
				FileInputStream fileInStream = new FileInputStream(fullPath);
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
	    						String Replace = str.replace("` (", "����").replace(")", ")/").replace("` ", "|").replace(" `", "");
		    					//String SubStr = str.replace(" `", "").replace("` ", "/").replace(") ", ")/").replace(" ", "");
		    					if(str.contains("datetime")) {
		    						Replace = str.replace("datetime", "datetime/").replace("` ", "|").replace(" `", "");
		    						//System.out.println(Replace);
		    					} else if (str.contains("DATATIME")) {
		    						Replace = str.replace("DATATIME", "DATATIME/").replace("` ", "|").replace(" `", "");
		    						//System.out.println(Replace);
		    					} else if(str.contains("timestamp")) {
		    						Replace = str.replace("timestamp", "timestamp/").replace("` ", "|").replace(" `", "");
		    					} else if(str.contains("TIMESTAMP")) {
		    						Replace = str.replace("TIMESTAMP", "TIMESTAMP/").replace("` ", "|").replace(" `", "");
		    					}
			    				//System.out.println("re : " + Replace);
			    				//System.out.println(Replace);
			    				if (Replace.contains("����")) {
			    					continue;
			    				}
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
			}
			//System.out.println("��� : " + Result);
			
	        FileOutputStream fileOutStream = new FileOutputStream(SavePath);
			OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
			BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
			bufferedWriter.write(Result);
			bufferedWriter.close();
		} catch (Exception e) {
			System.out.println("unkonwn error");
			e.printStackTrace();
		}
		 
	} /** * ��� ȣ���� �̿��Ͽ� ���� ������ Ž���Ѵ� * @param folderPath */ 
	
	public static void scanDir(String folderPath, List<String> fileLst) { 
		File[] files = new File(folderPath).listFiles(); 
		
		for(File f : files) { 
			if(f.isDirectory()) { 
				scanDir(f.getAbsolutePath(), fileLst); 
			} else { 
				fileLst.add(f.getAbsolutePath()); 
			} 
		}
	}
}

