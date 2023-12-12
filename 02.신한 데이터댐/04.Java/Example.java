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
		String dirPath = "C:\\신한 데이터댐\\01.업무\\04.외부데이터 테이블 정의서_20211103\\외부데이터\\새 폴더 (2)";
		
		showFilesInDIr(dirPath);
	}
	public static void showFilesInDIr(String dirPath) {
		try {
			File dir = new File(dirPath);
		    File files[] = dir.listFiles();
		    String Result = "";
		    String filename = "";
		    System.out.println("try가 몇번을 타는거야");
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
		    				
		    			if(str.contains("테이블명")) {
		    				if(str.contains("시트")) {
		    					String TableK = str.replace(") : ", "/");
			    				//System.out.println("뭐" + Repl);
			    				//System.out.println("시발" + Repl.length());
			    				String SubS = TableK.substring(TableK.indexOf("/")+1, TableK.length());
			    				TableKorean = SubS;
			    				//System.out.println("테이블한글명 : " + TableKorean);
		    				}
		    				if(str.contains("영문")) {
		    					String TableE = str.replace(") : ", "/");
			    				//System.out.println("뭐" + Repl);
			    				//System.out.println("시발" + Repl.length());
			    				String SubS = TableE.substring(TableE.indexOf("/")+1, TableE.length());
			    				TableEnglish = SubS;
			    				//System.out.println("테이블영문명 : " + TableEnglish);
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
				    					//System.out.println("뭐가 : " + Extract+ ":" + filename);
				    					//System.out.println("결과 : " + Result);
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
    				//System.out.println("카운트 : "+Count);
		    		System.out.println("결과 : "+ i + Result);
		    		}
		        }
		    System.out.println("file :" + filename);
	        //System.out.println("결과 : "+ i + Result);
	        String SavePath = "C:\\신한 데이터댐\\01.업무\\04.외부데이터 테이블 정의서_20211103\\외부데이터\\추출\\" + filename + ".sql";
	        FileOutputStream fileOutStream = new FileOutputStream(SavePath);
			OutputStreamWriter outputStreamReader = new OutputStreamWriter(fileOutStream, "UTF8");
			BufferedWriter bufferedWriter = new BufferedWriter(outputStreamReader);
			bufferedWriter.write(Result);
			bufferedWriter.close();
		    //ExtractResult += Result + "\n";
		    //System.out.println("결과 : " + ExtractResult);
		    //BufferedOutputStream allFileWriter = new BufferedOutputStream(new FileOutputStream(SavePath));
		    //allFileWriter.write(Result.getBytes());
		    //allFileWriter.close();
		    
		} catch (Exception e) {
			System.out.println("unkonwn error");
			e.printStackTrace();
		}
	    
	}
}
