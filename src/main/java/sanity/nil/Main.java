package sanity.nil;

import sanity.nil.problems.DirtyRead;
import sanity.nil.problems.FantomRead;
import sanity.nil.problems.LostUpdate;
import sanity.nil.problems.UnrepeatableRead;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Main {

    public static void main(String[] args) {
        Connection connection1 = null;
        Connection connection2 = null;
        try {
            connection1 = JdbcConnection.getConn();
            connection2 = JdbcConnection.getConn();

            StringBuilder sb = new StringBuilder();
            sb.append("DROP TABLE IF EXISTS my_table;\n");
            sb.append("CREATE TABLE IF NOT EXISTS my_table (id INT PRIMARY KEY, value INT);\n");
            for (int i = 1; i < 11; i++) {
                sb.append(String.format("INSERT INTO my_table (id, value) VALUES (%d, 10);\n", i));
            }

            PreparedStatement createTableStatement = connection1.prepareStatement(sb.toString());
            createTableStatement.execute();

            connection1.setAutoCommit(false);
            connection2.setAutoCommit(false);

            // Choose and uncomment one of the below methods

//            DirtyRead.dirtyReadAnomaly(connection1, connection2);
//            DirtyRead.dirtyReadFix(connection1, connection2);
//            LostUpdate.lostUpdateAnomaly(connection1, connection2);
//            LostUpdate.lostUpdateFix(connection1, connection2);
            UnrepeatableRead.unrepeatableReadAnomaly(connection1, connection2);
//            UnrepeatableRead.unrepeatableReadFix(connection1, connection2);
//            FantomRead.fantomReadAnomaly(connection1, connection2);
//            FantomRead.fantomReadFix(connection1, connection2);
            System.out.println("Finished execution");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                connection1.close();
                connection2.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}