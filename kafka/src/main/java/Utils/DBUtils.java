package Utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

public class DBUtils {
    public static Connection connection = getMysqlConnection();

    public static Connection getMysqlConnection() {
        Map<String, String> map = FileUtils.getProperties("");
        String url = map.get("mysqlUrl");
        String username = map.get("mysqlUser");
        String password = map.get("mysqlPassWord");
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection= DriverManager.getConnection(url, username, password);
        } catch (SQLException | ClassNotFoundException throwables) {
            throwables.printStackTrace();
        }
        return connection;
    }
}
