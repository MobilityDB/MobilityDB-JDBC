package edu.ulb.mobilitydb.jdbc.integration.tbool;

import edu.ulb.mobilitydb.jdbc.integration.BaseIntegrationTest;
import edu.ulb.mobilitydb.jdbc.tbool.TBool;
import edu.ulb.mobilitydb.jdbc.tbool.TBoolInst;
import edu.ulb.mobilitydb.jdbc.tbool.TBoolSeq;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;


class TBoolSeqTest extends BaseIntegrationTest {
    @Test
    void testStringConstructor() throws Exception {
        String value = "[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02]";
        TBool tBool = new TBool(value);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tbool (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tBool);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tbool WHERE temporaltype=?;");
        readStatement.setObject(1, tBool);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TBool retrievedTBool = (TBool) rs.getObject(1);
            assertEquals(tBool.getTemporal(), retrievedTBool.getTemporal());
        } else {
            fail("TBool was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testStringsIncUppDefaultConstructor() throws Exception {
        String[] values = new String[] {"true@2001-01-01 08:00:00+02", "true@2001-01-03 08:00:00+02"};

        TBoolSeq tBoolSeq = new TBoolSeq(values);
        TBool tBool = new TBool(tBoolSeq);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tbool (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tBool);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tbool WHERE temporaltype=?;");
        readStatement.setObject(1, tBool);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TBool retrievedTBool = (TBool) rs.getObject(1);
            assertEquals(tBool.getTemporal(), retrievedTBool.getTemporal());
        } else {
            fail("TBool was not retrieved.");
        }
    }

    @Test
    void testStringsIncUppConstructor() throws Exception {
        String[] values = new String[] {"1@2001-01-01 08:00:00+02", "1@2001-01-03 08:00:00+02"};

        TBoolSeq tBoolSeq = new TBoolSeq(values, true, true);
        TBool tBool = new TBool(tBoolSeq);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tbool (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tBool);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tbool WHERE temporaltype=?;");
        readStatement.setObject(1, tBool);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TBool retrievedTBool = (TBool) rs.getObject(1);
            assertEquals(tBool.getTemporal(), retrievedTBool.getTemporal());
        } else {
            fail("TBool was not retrieved.");
        }
    }

    @Test
    void testInstantsIncUppDefaultConstructor() throws Exception {
        TBoolInst[] values = new TBoolInst[] {new TBoolInst("true@2001-01-01 08:30:00+02"),
                new TBoolInst("false@2001-01-03 18:00:00+02"), new TBoolInst("false@2001-01-03 20:20:00+02")};

        TBoolSeq tBoolSeq = new TBoolSeq(values);
        TBool tBool = new TBool(tBoolSeq);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tbool (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tBool);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tbool WHERE temporaltype=?;");
        readStatement.setObject(1, tBool);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TBool retrievedTBool = (TBool) rs.getObject(1);
            assertEquals(tBool.getTemporal(), retrievedTBool.getTemporal());
        } else {
            fail("TBool was not retrieved.");
        }
    }

    @Test
    void testInstantsIncUppConstructor() throws Exception {
        TBoolInst[] values = new TBoolInst[] {new TBoolInst("false@2001-01-01 08:30:00+02"),
                new TBoolInst("true@2001-01-03 18:00:00+02"), new TBoolInst("true@2001-01-03 20:20:00+02")};

        TBoolSeq tBoolSeq = new TBoolSeq(values, true, false);
        TBool tBool = new TBool(tBoolSeq);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tbool (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tBool);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tbool WHERE temporaltype=?;");
        readStatement.setObject(1, tBool);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TBool retrievedTBool = (TBool) rs.getObject(1);
            assertEquals(tBool.getTemporal(), retrievedTBool.getTemporal());
        } else {
            fail("TBool was not retrieved.");
        }
    }
}
