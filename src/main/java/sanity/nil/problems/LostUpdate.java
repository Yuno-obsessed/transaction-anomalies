package sanity.nil.problems;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LostUpdate {

    public static void lostUpdateAnomaly(Connection connection1, Connection connection2) {
        lostUpdate(connection1, connection2, false);
    }

    public static void lostUpdateFix(Connection connection1, Connection connection2) {
        lostUpdate(connection1, connection2, true);
    }

    private static void lostUpdate(Connection connection1, Connection connection2, boolean isFixed) {

        try {

            connection1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

            for (int i = 1; i < 11; i++) {
                int finalI = i;

                // This thread updates a value and waits for a concurrent connection to read
                // a value before current thread commits changes
                Thread thread1 = new Thread(() -> {
                    try {
                        executeTransaction(connection1, finalI, 10, isFixed);
                        sleep(connection1, .5);
                        connection1.commit();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });

                // This thread simply updates a value and commits, so lost update will
                // occur only if thread1 updates before thread2 commits (not always, but often).
                Thread thread2 = new Thread(() -> {
                    try {
                        executeTransaction(connection2, finalI, 20, isFixed);
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

    private static void executeTransaction(Connection connection, int i, int plus, boolean isFixed) throws SQLException {
        String select;
        if (isFixed) {
            select = String.format("SELECT value FROM my_table WHERE id = %d FOR UPDATE;", i);
        } else {
            select = String.format("SELECT value FROM my_table WHERE id = %d;", i);
        }
        ResultSet resultSet = connection.prepareStatement(select).executeQuery();
        resultSet.next();
        int value = resultSet.getInt("value");
        System.out.println(String.format("%s: Value = %d", Thread.currentThread().getName(), value));
        String update = String.format("UPDATE my_table SET value = %d + %d WHERE id = %d;", value, plus, i);
        System.out.println(String.format("%s: Executing SQL: %s",Thread.currentThread().getName(), update));
        connection.prepareStatement(update).execute();
    }

    private static void sleep(Connection connection, double duration) throws SQLException {
        connection.prepareStatement(String.format("SELECT pg_sleep(%f);", duration)).executeQuery();
    }

}
