package entity;

import java.sql.Timestamp;

/**
 * ClassName UrlCountEnd
 *
 * @Auther: 赵繁旗
 * @Date: 2022/4/26 17:27
 * @Description:
 */
public class UrlCountEnd {
    public String url;
    public  Integer count;
    public Long windowEnd;

    public UrlCountEnd(String url, Integer count, Long windowEnd) {
        this.url = url;
        this.count = count;
        this.windowEnd = windowEnd;
    }

    public UrlCountEnd() {
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "UrlCountEnd{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", windowEnd=" +  new Timestamp(windowEnd) +
                '}';
    }
}
