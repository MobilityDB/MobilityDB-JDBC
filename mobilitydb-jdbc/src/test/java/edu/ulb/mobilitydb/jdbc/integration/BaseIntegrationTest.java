package edu.ulb.mobilitydb.jdbc.integration;

import edu.ulb.mobilitydb.jdbc.DataTypeHandler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.postgresql.PGConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class BaseIntegrationTest {
    private static final String url = "jdbc:postgresql://localhost:25432/mobilitydb";
    private static final String user = "docker";
    private static final String password = "docker";
    protected static Connection con;

    private static final String[] timeTypes = new String[] { "period", "periodset" };

    @BeforeAll
    static void connectionSetup() throws SQLException {
        con = DriverManager.getConnection(url, user, password);
        PGConnection pgConnection = (PGConnection) con;
        DataTypeHandler.INSTANCE.registerTypes(pgConnection);

        for (String type : timeTypes) {
            createTimeTable(type);
        }
    }

    @AfterAll
    static void connectionDispose() throws SQLException {
        if (con != null) {
            for (String type : timeTypes) {
                dropTable(type);
            }

            con.close();
        }
    }

    @BeforeEach
    void init() throws SQLException {
        for (String type : timeTypes) {
            clearTimeTable(type);
        }
    }

    public static void createTimeTable(String type) throws SQLException {
        String command = String.format("CREATE TABLE IF NOT EXISTS tbl_%1$s (timetype %1$s NOT NULL);", type);
        Statement st = con.createStatement();
        st.execute(command);
        st.close();
    }

    private static void clearTimeTable(String type) throws SQLException {
        String command = String.format("TRUNCATE TABLE tbl_%s;", type);
        Statement st = con.createStatement();
        st.execute(command);
        st.close();
    }

    public static void dropTable(String type) throws SQLException {
        String command = String.format("DROP TABLE tbl_%s;", type);
        Statement st = con.createStatement();
        st.execute(command);
        st.close();
    }
}
