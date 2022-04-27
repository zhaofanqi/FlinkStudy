package entity;

import java.sql.Timestamp;

/**
 * ClassName Order
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/27 20:23
 * @Description:
 */
public class Order {
    public String user;//用户
    public String address;// 地点
    public Long timestamp;// 时间

    public Order() {
    }

    public Order(String user, String address, Long timestamp) {
        this.user = user;
        this.address = address;
        this.timestamp = timestamp;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Order{" +
                "user='" + user + '\'' +
                ", address='" + address + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
