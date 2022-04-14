package entity;

import java.sql.Timestamp;

/**
 * ClassName Click
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/13 15:02
 * @Description:  模拟用户点击页面的pojo类
 */
public class Click {
    public String user;
    public String url;
    public Long timeStamp;

    public Click() {
    }

    public Click(String user, String url, Long timeStamp) {
        this.user = user;
        this.url = url;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "Click{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timeStamp=" + new Timestamp(timeStamp) +
                '}';
    }
}


