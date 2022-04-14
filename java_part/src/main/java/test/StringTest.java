package test;


import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class StringTest {
    public static void main(String[] args) {
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateString = simpleDateFormat.format(date);
        System.out.println(date);
        System.out.println(dateString);
        //构件定长字符串 1024
        int length=1024;
        char c='c';
//        specifyLengthString(length,c);
    }

    public static String specifyLengthString(int length,char c) {
        char[] chars = new char[length];
        Arrays.fill(chars,c);
//        System.out.println(chars.length);
//        System.out.println(Arrays.toString(chars));
        String str = Arrays.toString(chars)
                .replace(",", "")
                .replace("[", "")
                .replace("]", "")
                .replace(" ","");
        System.out.println(str);
//        System.out.println(str.length());
        return str;
    }

}
