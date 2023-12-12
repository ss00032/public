public class GenTera {

              public static void main(String args[]) {

                            cbsConnection stgDdl \u003d new cbsConnection();
                            String cbsDdl \u003d stgDdl.CBSDDL();

                            GenTeraBlock teraBlock \u003d new GenTeraBlock();
                            String block \u003d teraBlock.Terablock();

                            GenTeraTs teraTs \u003d new GenTeraTs();
                            String ts \u003d teraTs.TeraTs();
              }
}