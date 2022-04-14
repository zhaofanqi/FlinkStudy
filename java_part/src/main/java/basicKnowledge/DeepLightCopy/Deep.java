package basicKnowledge.DeepLightCopy;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.*;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Deep implements Cloneable,Serializable {
    private String deepName;
    private Integer deepAge;
    private Mid mid;

    /**
     *     流的方式实现深复制
     *     new 对象的方式实现深复制，但注意引用对象需要再次new噢
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        Deep deep = new Deep(this.deepName, this.deepAge, new Mid(mid.getMidName()));
//        Deep deep = new Deep(this.deepName, this.deepAge, this.mid);//这种方式不行，对象初始化给的是另外一个对象的引用
        return deep;

        /*Object result=null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(this);
            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bis);
            result = ois.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return result;*/
    }
}
