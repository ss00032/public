import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class cbsDBConnection {
	public static Connection dbConn;

	public static Connection getConnection() {
		Connection connCbs = null;
		try {

			String cbsUser = "810257";
			String cbsPw = "tossbk_24";
			String cbsUrl = "jdbc:oracle:thin:@10.52.11.132:1529/PCOR";
			// jdbc �����Ҷ� tnsname�� �����ʴ´�.
			// tnsname error�� �㶩 Use Service Name�� ������ Ȯ��.
			// User Service Name�� ����Ҷ� / �� ����Ͽ� ���� / =\u003e Service Name�� ���
			// ������� ������ : �� ���. : =\u003e SID�� ���

			Class.forName("oracle.jdbc.driver.OracleDriver");
			connCbs = DriverManager.getConnection(cbsUrl, cbsUser, cbsPw);

			System.out.println("����");
		} catch (ClassNotFoundException cnfe) {
			System.out.println("�ε����� :" + cnfe.toString());
		} catch (SQLException sqle) {
			System.out.println("���ӽ��� :" + sqle.toString());
		} catch (Exception e) {
			System.out.println("unkonwn error");
			e.printStackTrace();
		}
		return connCbs;
	}
}
