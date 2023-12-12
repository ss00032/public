public class rPad {

	public static String padRight(String word, int totalWidth, char paddingChar) {

		String padWord = word;
		padWord = String.format("%-" + totalWidth + "s", word).replace(' ', paddingChar);
		return padWord;
	}
}
