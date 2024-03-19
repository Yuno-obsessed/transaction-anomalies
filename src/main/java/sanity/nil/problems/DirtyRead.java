package sanity.nil.problems;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DirtyRead {

    public static void dirtyReadAnomaly(Connection connection1, Connection connection2) {
        dirtyRead(connection1, connection2, false);
    }

    public static void dirtyReadFix(Connection connection1, Connection connection2) {
        dirtyRead(connection1, connection2, true);
    }

    // Is not supported in PG because of its mechanisms of transactions, so even on the lowest
    // Read Committed isolation level we can't see the results of a concurrent not committed transaction
    public static void dirtyRead(Connection connection1, Connection connection2, boolean isFixed) {

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

                // This thread updates a record and waits before rolling back.
                Thread thread1 = new Thread(() -> {
                    try {
                        executeTransaction(connection1, finalI);
                        sleep(connection1, 1.5);
                        connection1.rollback();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });

                // This thread reads a value, waits for concurrent transaction to update a record
                // and then tries to read updated but not committed changed value (not possible)
                Thread thread2 = new Thread(() -> {
                    try {
                        readValue(connection2, finalI);
                        sleep(connection2, 0.5);
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

            PreparedStatement preparedStatement = connection1.prepareStatement("SELECT * FROM my_table;");
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                int value = resultSet.getInt("value");
                System.out.println("Value = " + value);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void readValue(Connection connection, int i) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(String.format("SELECT value FROM my_table WHERE id = %d;", i));
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        int value = resultSet.getInt("value");
        System.out.println(String.format("%s: Value: %d", Thread.currentThread().getName(), value));
    }

    private static void executeTransaction(Connection connection, int id) throws SQLException{
        String update = String.format("UPDATE my_table SET value = value + 10 WHERE id = %d;", id);
        System.out.println(String.format("%s: Executing SQL: %s", Thread.currentThread().getName(), update));
        connection.prepareStatement(update).executeUpdate();
    }

    private static void sleep(Connection connection, double duration) throws SQLException {
        connection.prepareStatement(String.format("SELECT pg_sleep(%f);", duration)).executeQuery();
    }
}
