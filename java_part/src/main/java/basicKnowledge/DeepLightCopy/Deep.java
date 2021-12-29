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

    @Override
    protected Object clone() throws CloneNotSupportedException {
        Object result=null;
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
        return result;
    }
}
