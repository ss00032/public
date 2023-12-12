public class rPad {

public static String padRight(String word, int totalWidth, char paddingChar) {

String padWord \u003d word;
padWord \u003d String.format(\""%-\"" + totalWidth + \""s\"", word).replace(\u0027 \u0027, paddingChar);
return padWord;
}
}
