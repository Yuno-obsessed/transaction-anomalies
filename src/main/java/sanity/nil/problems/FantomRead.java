package sanity.nil.problems;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class FantomRead {

    public static void fantomReadAnomaly(Connection connection1, Connection connection2) {
        fantomRead(connection1, connection2, false);
    }

    public static void fantomReadFix(Connection connection1, Connection connection2) {
        fantomRead(connection1, connection2, true);
    }

    private static void fantomRead(Connection connection1, Connection connection2, boolean isFixed) {
        try {

            if (isFixed) {
                connection1.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                connection2.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            } else {
                connection1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                connection2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            }

            for (int i = 1; i < 11; i++) {
                int finalI = i;

                // This thread waits a little to let concurrent transaction count records for the first time,
                // then inserts a new record in table and commits
                Thread thread1 = new Thread(() -> {
                    try {
                        Thread.sleep(500);
                        executeTransaction(connection1, 10 + finalI);
                        connection1.commit();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                // This thread counts records, then waits for a concurrent transaction to insert a new record
                // and counts again
                Thread thread2 = new Thread(() -> {
                    try {
                        readValue(connection2);
                        sleep(connection2);
                        readValue(connection2);
                        connection2.commit();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
                thread1.start();
                thread2.start();

                thread1.join();
                thread2.join();
                System.out.println();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void executeTransaction(Connection connection, int id) throws SQLException {
        String insert = String.format("INSERT INTO my_table(id, value) VALUES (%d, 10);", id);
        PreparedStatement preparedStatement = connection.prepareStatement(insert);
        System.out.println(String.format("%s: Executing SQL: %s",Thread.currentThread().getName(), insert));
        preparedStatement.executeUpdate();
    }

    private static void readValue(Connection connection) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT count(*) FROM my_table;");
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        int value = resultSet.getInt("count");
        System.out.println(String.format("%s: Value: %d", Thread.currentThread().getName(), value));
    }

    private static void sleep(Connection connection) throws SQLException {
        connection.prepareStatement("SELECT pg_sleep(1.0);").executeQuery();
    }
}
