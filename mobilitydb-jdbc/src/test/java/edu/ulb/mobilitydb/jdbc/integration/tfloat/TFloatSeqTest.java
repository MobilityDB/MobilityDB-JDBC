package edu.ulb.mobilitydb.jdbc.integration.tfloat;

import edu.ulb.mobilitydb.jdbc.integration.BaseIntegrationTest;
import edu.ulb.mobilitydb.jdbc.tfloat.TFloat;
import edu.ulb.mobilitydb.jdbc.tfloat.TFloatInst;
import edu.ulb.mobilitydb.jdbc.tfloat.TFloatSeq;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TFloatSeqTest extends BaseIntegrationTest {

    @Test
    void testStringConstructor() throws Exception {
        String value = "[1.8@2001-01-01 08:00:00+02, 1.9@2001-01-03 08:00:00+02]";

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
    void testStringsIncUppDefaultConstructor() throws Exception {
        String[] values = new String[] {"1.8@2001-01-01 08:00:00+02", "1.9@2001-01-03 08:00:00+02"};

        TFloatSeq tFloatSeq = new TFloatSeq(values);
        TFloat tFloat = new TFloat(tFloatSeq);

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
    void testStringsIncUppConstructor() throws Exception {
        String[] values = new String[] {"1.8@2001-01-01 08:00:00+02", "1.9@2001-01-03 08:00:00+02"};

        TFloatSeq tFloatSeq = new TFloatSeq(values, true, true);
        TFloat tFloat = new TFloat(tFloatSeq);

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
    void testInstantsIncUppDefaultConstructor() throws Exception {
        TFloatInst[] values = new TFloatInst[] {new TFloatInst("25.3@2001-01-01 08:30:00+02"),
                new TFloatInst("76.1@2001-01-03 18:00:00+02"), new TFloatInst("76.1@2001-01-03 20:20:00+02")};
        TFloatSeq tFloatSeq = new TFloatSeq(values);
        TFloat tFloat = new TFloat(tFloatSeq);

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
    void testInstantsIncUppConstructor() throws Exception {
        TFloatInst[] values = new TFloatInst[] {new TFloatInst("25.3@2001-01-01 08:30:00+02"),
                new TFloatInst("76.1@2001-01-03 18:00:00+02"), new TFloatInst("76.1@2001-01-03 20:20:00+02")};
        TFloatSeq tFloatSeq = new TFloatSeq(values, true, true);
        TFloat tFloat = new TFloat(tFloatSeq);

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
