public class genTera {

	public static void main(String args[]) {

		CBSConnection stgDdl = new CBSConnection();
		String cbsDdl = stgDdl.CBSDDL();

		genTeraBlock teraBlock = new genTeraBlock();
		String block = teraBlock.Terablock();

		genTeraTs teraTs = new genTeraTs();
		String ts = teraTs.TeraTs();
	}
}