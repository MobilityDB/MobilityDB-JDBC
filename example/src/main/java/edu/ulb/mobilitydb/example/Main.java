package edu.ulb.mobilitydb.example;

import edu.ulb.mobilitydb.jdbc.DataTypeHandler;
import edu.ulb.mobilitydb.jdbc.time.Period;
import org.postgresql.PGConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:25432/mobilitydb";
        String user = "docker";
        String password = "docker";

        try {
            Connection con = DriverManager.getConnection(url, user, password);
            // Add the JDBC extension object
            PGConnection pgconn = (PGConnection) con;
            DataTypeHandler.INSTANCE.registerTypes(pgconn);

            String command = "SELECT period ('2000-01-01','2000-01-02', false, true)";
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
            con.close();
        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(Main.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }
}
