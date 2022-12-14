package com.mobilitydb.jdbc.integration.tfloat;

import com.mobilitydb.jdbc.integration.BaseIntegrationTest;
import com.mobilitydb.jdbc.tfloat.TFloat;
import com.mobilitydb.jdbc.tfloat.TFloatInst;
import com.mobilitydb.jdbc.tfloat.TFloatInstSet;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TFloatInstSetTest extends BaseIntegrationTest {
    @Test
    void testStringConstructor() throws Exception {
        String value = "{1.0@2001-01-01 08:00:00+03, 2.0@2001-01-03 08:00:00+03}";
        TFloat tFloat = new TFloat(value);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tfloat (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tFloat);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tfloat WHERE temporaltype=?;");
        readStatement.setObject(1, tFloat);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TFloat retrievedTFloat = (TFloat) rs.getObject(1);
            assertEquals(tFloat.getTemporal(), retrievedTFloat.getTemporal());
        } else {
            fail("TFloat was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testStringsConstructor() throws Exception {
        String[] values = new String[] {"23.3@2001-01-01 08:20:00+03", "48.6@2001-01-03 18:08:00+03",
                "76.6@2001-01-03 20:20:00+03"};

        TFloatInstSet tFloatInstSet = new TFloatInstSet(values);
        TFloat tFloat = new TFloat(tFloatInstSet);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tfloat (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tFloat);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tfloat WHERE temporaltype=?;");
        readStatement.setObject(1, tFloat);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TFloat retrievedTFloat = (TFloat) rs.getObject(1);
            assertEquals(tFloat.getTemporal(), retrievedTFloat.getTemporal());
        } else {
            fail("TFloat was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testInstantsConstructor() throws Exception {
        TFloatInst[] values = new TFloatInst[] {new TFloatInst("25.2@2001-01-01 08:30:00+02"),
                new TFloatInst("48.9@2001-01-03 18:00:00+02"), new TFloatInst("76.4@2001-01-03 20:20:00+02")};

        TFloatInstSet tFloatInstSet = new TFloatInstSet(values);
        TFloat tFloat = new TFloat(tFloatInstSet);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tfloat (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tFloat);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tfloat WHERE temporaltype=?;");
        readStatement.setObject(1, tFloat);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TFloat retrievedTFloat = (TFloat) rs.getObject(1);
            assertEquals(tFloat.getTemporal(), retrievedTFloat.getTemporal());
        } else {
            fail("TFloat was not retrieved.");
        }

        readStatement.close();
    }
}
