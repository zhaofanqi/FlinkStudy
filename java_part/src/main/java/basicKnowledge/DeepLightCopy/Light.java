package basicKnowledge.DeepLightCopy;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Light implements Serializable,Cloneable {
    private String lightName;
    private int lightAge;
    private Mid mid;

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
