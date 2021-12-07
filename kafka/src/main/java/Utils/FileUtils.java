package Utils;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

public class FileUtils {
    public static void main(String[] args) {
        //测试 读取文件
        Map<String, String> map = getProperties("conf");
        for (String key : map.keySet()) {
            System.out.println(key+"\t"+map.get(key));
        }
    }
    public static Map<String, String> getProperties(String fileName) {
        Map<String, String> map = new HashMap<>();
        if ("".equals(fileName)&&fileName==null){
            fileName="conf";
        }
        ResourceBundle bundle = ResourceBundle.getBundle(fileName);
        Enumeration<String> keys = bundle.getKeys();
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            map.put(key, bundle.getString(key));
        }
        return map;

    }
}
