public static Connection dbConn;

              public static Connection getConnection() {
                            Connection connCbs \u003d null;
                            try {

                                          String cbsUser \u003d \"810257\";
                                          String cbsPw \u003d \"tossbk_24\";
                                          String cbsUrl \u003d \"jdbc:oracle:thin:@10.52.11.132:1529/PCOR\";
                                          // jdbc 연결할땐 tnsname을 쓰지않는다.
                                          // tnsname error가 뜰땐 Use Service Name을 쓰는지 확인.
                                          // User Service Name을 사용할땐 / 을 사용하여 연결    /  \u003d\u003e Service Name을 사용
                                          // 사용하지 않을땐 : 을 사용.      :   \u003d\u003e  SID를 사용

                                          Class.forName(\"oracle.jdbc.driver.OracleDriver\");
                                          connCbs \u003d DriverManager.getConnection(cbsUrl, cbsUser, cbsPw);

                                          System.out.println(\"연결.\
\");
                            } catch (ClassNotFoundException cnfe) {
                                          System.out.println(\"로딩실패 :\"+cnfe.toString());
                            } catch (SQLException sqle) {
                                          System.out.println(\"접속실패 :\"+sqle.toString());
                            } catch (Exception e) }
                                          System.out.println(\"unkonwn error\");
                                          e.printStackTrace();
                            }
                            return connCbs;
              }
}
