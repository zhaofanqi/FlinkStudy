package SeriParInter;


import entities.Company;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * ClassName MySerializer
 * 案例： kafka传递的是Company 对象
 *
 * @Auther: 赵繁旗
 * @Date: 2021/12/4 19:33
 * @Description:
 */
public class MySerializer implements Serializer<Company> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Company data) {
        if (data == null) {
            return null;
        }
        byte[] name, address;
        try {
            if (data.getCompanyName() != null) {
                name=data.getCompanyName().getBytes("utf-8");
            }else {
             name=new byte[0];
            }
            if (data.getCompanyAddress()!=null){
                address=data.getCompanyAddress().getBytes("utf-8");
            }else {
                address=new byte[0];
            }
            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + name.length + address.length);
            byteBuffer.putInt(name.length);
            byteBuffer.put(name);
            byteBuffer.putInt(address.length);
            byteBuffer.put(address);
            return byteBuffer.array();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }


        return new byte[0];
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Company data) {
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
