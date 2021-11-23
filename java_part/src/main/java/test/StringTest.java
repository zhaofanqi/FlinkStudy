package test;



import java.util.Arrays;

public class StringTest {
    public static void main(String[] args) {
        //构件定长字符串 1024
        int length=1024;
        char c='c';
        specifyLengthString(length,c);
    }

    public static String specifyLengthString(int length,char c) {
        char[] chars = new char[length];
        Arrays.fill(chars,c);
//        System.out.println(chars.length);
//        System.out.println(Arrays.toString(chars));
        String replace = Arrays.toString(chars)
                .replace(",", "")
                .replace("[", "")
                .replace("]", "")
                .replace(" ","");
        System.out.println(replace);
        System.out.println(replace.length());
        return replace;
    }
}
