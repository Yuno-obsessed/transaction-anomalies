package sanity.nil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcConnection {

    public static Connection getConn() {
        String url = "jdbc:postgresql://localhost:5436/postgres";
        String user = "sanity";
        String password = "sanity";
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create a database connection.", e);
        }
    }
}
