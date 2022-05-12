package entity;

/**
 * ClassName LoginEvent
 *
 * @Auther: 赵繁旗
 * @Date: 2022/5/11 08:59
 * @Description:
 */
public class LoginEvent {
    public String userName;
    public String loginUrl;
    public String loginStatus;
    public long loginTimeStamp;

    public LoginEvent() {
    }

    public LoginEvent(String userName, String loginUrl, String loginStatus, long loginTimeStamp) {
        this.userName = userName;
        this.loginUrl = loginUrl;
        this.loginStatus = loginStatus;
        this.loginTimeStamp = loginTimeStamp;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getLoginUrl() {
        return loginUrl;
    }

    public void setLoginUrl(String loginUrl) {
        this.loginUrl = loginUrl;
    }

    public String getLoginStatus() {
        return loginStatus;
    }

    public void setLoginStatus(String loginStatus) {
        this.loginStatus = loginStatus;
    }

    public long getLoginTimeStamp() {
        return loginTimeStamp;
    }

    public void setLoginTimeStamp(long loginTimeStamp) {
        this.loginTimeStamp = loginTimeStamp;
    }


    @Override
    public String toString() {
        return "LoginEvent{" +
                "userName='" + userName + '\'' +
                ", loginUrl='" + loginUrl + '\'' +
                ", loginStatus='" + loginStatus + '\'' +
                ", loginTimeStamp=" + loginTimeStamp +
                '}';
    }
}
