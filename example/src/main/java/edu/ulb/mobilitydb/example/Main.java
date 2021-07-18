package edu.ulb.mobilitydb.example;

import edu.ulb.mobilitydb.jdbc.DataTypeHandler;
import edu.ulb.mobilitydb.jdbc.time.Period;
import org.postgresql.PGConnection;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final String url = "jdbc:postgresql://localhost:25432/mobilitydb";
    private static final String user = "docker";
    private static final String password = "docker";

    public static void main(String[] args) {
        try {
            Connection con = DriverManager.getConnection(url, user, password);
            // Add the JDBC extension object
            PGConnection pgconn = (PGConnection) con;
            DataTypeHandler.INSTANCE.registerTypes(pgconn);

            createTable(con);
            clearTable(con);
            testInsertPeriod(con);
            testReadPeriod(con);
            con.close();
        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(Main.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }

    public static void testInsertPeriod(Connection con) throws SQLException {
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01]");

        PreparedStatement statement = con.prepareStatement("INSERT INTO tbl_period (timetype) VALUES (?)");
        statement.setObject(1, period);
        statement.execute();
    }

    public static void testReadPeriod(Connection con) throws SQLException {
        String command = "SELECT timetype FROM tbl_period;";
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(command);

        if (rs.next()) {
            Period a = (Period)rs.getObject(1);
            System.out.format("Value retrieved: %s%n", a.toString());
            System.out.format("Is lower inclusive: %b%n", a.isLowerInclusive());
            System.out.format("Lower date: %s%n", a.getLower().toString());
            System.out.format("Is upper inclusive: %b%n", a.isUpperInclusive());
            System.out.format("Upper date: %s%n", a.getUpper().toString());
        }

        st.close();
    }

    public static void createTable(Connection con) throws SQLException {
        String command = "CREATE TABLE IF NOT EXISTS tbl_period (timetype period NOT NULL);";
        Statement st = con.createStatement();
        st.execute(command);
        st.close();
    }

    public static void clearTable(Connection con) throws SQLException {
        String command = "TRUNCATE TABLE tbl_period;";
        Statement st = con.createStatement();
        st.execute(command);
        st.close();
    }
}
