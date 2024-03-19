package sanity.nil.problems;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class UnrepeatableRead {

    public static void unrepeatableReadAnomaly(Connection connection1, Connection connection2) {
        unrepeatableRead(connection1, connection2, false);
    }

    public static void unrepeatableReadFix(Connection connection1, Connection connection2) {
        unrepeatableRead(connection1, connection2, true);
    }

    private static void unrepeatableRead(Connection connection1, Connection connection2, boolean isFixed) {
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

                // This thread waits for a concurrent transaction to read a value and then
                // updates a value and commits
                Thread thread1 = new Thread(() -> {
                    try {
                        Thread.sleep(500);
                        executeTransaction(connection1, finalI);
                        connection1.commit();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                // This thread reads value, waits for a concurrent transaction to update this value
                // and then reads an updated value
                Thread thread2 = new Thread(() -> {
                    try {
                        readValue(connection2, finalI);
                        sleep(connection2);
                        readValue(connection2, finalI);
                        connection2.commit();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
                thread1.start();
                thread2.start();

                thread1.join();
                thread2.join();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void executeTransaction(Connection connection, int id) throws SQLException {
        String update = String.format("UPDATE my_table SET value = value + 10 WHERE id = %d;", id);
        System.out.println(String.format("%s: Executing SQL: %s", Thread.currentThread().getName(), update));
        connection.prepareStatement(update).executeUpdate();
    }

    private static void readValue(Connection connection, int i) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(String.format("SELECT value FROM my_table WHERE id = %d;", i));
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        int value = resultSet.getInt("value");
        System.out.println(String.format("%s: Value: %d", Thread.currentThread().getName(), value));
    }

    private static void sleep(Connection connection) throws SQLException {
        connection.prepareStatement("SELECT pg_sleep(1.0);").executeQuery();
    }
}
