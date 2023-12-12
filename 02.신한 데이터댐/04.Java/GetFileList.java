import java.io.File;


public class GetFileList {

  public static void main(String[] args) {

    File myDir = new File("C:\\���� �����ʹ�\\01.����\\04.�ܺε����� ���̺� ���Ǽ�_20211103\\�ܺε�����"); // ���丮�� ��ü����
    System.out.println(myDir.getAbsolutePath());     			// ���丮�� ������
    System.out.println(myDir.getParent()); 				// �θ���丮
    String fileName = myDir.getName();                                    // ���� �̸�
    String fileExtention = fileName.substring(fileName.lastIndexOf(".")+1,fileName.length());    // ���� Ȯ����

    File[] contents = myDir.listFiles();					// ���丮�� ������ �����´�

    // ���丮�� ���� ������

    if (contents != null) {    // contents�� null���� �ƴҰ��(���丮�� ������ �������)���丮�� ������� 
      for (int i = 0; i < contents.length; i++) {
    	  System.out.println(contents[i].getName());
      }
    } else {                     // contents�� null�� ���(���丮�� ������ �������)
      System.out.println(myDir.getName() + " is not a directory");
    }
    System.exit(0);
  }
}