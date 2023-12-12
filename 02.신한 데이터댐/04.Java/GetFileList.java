import java.io.File;


public class GetFileList {

  public static void main(String[] args) {

    File myDir = new File("C:\\신한 데이터댐\\01.업무\\04.외부데이터 테이블 정의서_20211103\\외부데이터"); // 디렉토리의 객체생성
    System.out.println(myDir.getAbsolutePath());     			// 디렉토리의 절대경로
    System.out.println(myDir.getParent()); 				// 부모디렉토리
    String fileName = myDir.getName();                                    // 파일 이름
    String fileExtention = fileName.substring(fileName.lastIndexOf(".")+1,fileName.length());    // 파일 확장자

    File[] contents = myDir.listFiles();					// 디렉토리의 내용을 가져온다

    // 디렉토리의 내용 목록출력

    if (contents != null) {    // contents가 null값이 아닐경우(디렉토리에 내용이 있을경우)디렉토리의 내용출력 
      for (int i = 0; i < contents.length; i++) {
    	  System.out.println(contents[i].getName());
      }
    } else {                     // contents가 null일 경우(디렉토리에 내용이 없을경우)
      System.out.println(myDir.getName() + " is not a directory");
    }
    System.exit(0);
  }
}