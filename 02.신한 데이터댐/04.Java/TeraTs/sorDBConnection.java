import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

// \u003d -> =
// \u0027 -> '
// \u0026 -> &
// \u003e -> >
// \u003c -> <
public class sorDBConnection {

	public static Connection dbConn;

	public static Connection getConnection() {
		Connection connSor = null;
		try {

			String sorUser = "810257";
			String sorPw = "tossbk_24";
			String sorUrl = "jdbc:oracle:thin:@10.57.151.33:1529:DEDW";
			// jdbc �����Ҷ� tnsname�� �����ʴ´�.
			// tnsname error�� �㶩 Use Service Name�� ������ Ȯ��.
			// User Service Name�� ����Ҷ� / �� ����Ͽ� ���� / =\u003e Service Name�� ���
			// ������� ������ : �� ���. : =\u003e SID�� ���

			Class.forName("oracle.jdbc.driver.OracleDriver");
			connSor = DriverManager.getConnection(sorUrl, sorUser, sorPw);

			System.out.println("����");
		} catch (ClassNotFoundException cnfe) {
			System.out.println("�ε����� :" + cnfe.toString());
		} catch (SQLException sqle) {
			System.out.println("���ӽ��� :" + sqle.toString());
		} catch (Exception e) {
			System.out.println("unkonwn error");
			e.printStackTrace();
		}
		return connSor;
	}
}
