package com.mobilitydb.jdbc.integration.tbool;

import com.mobilitydb.jdbc.integration.BaseIntegrationTest;
import com.mobilitydb.jdbc.tbool.TBool;
import com.mobilitydb.jdbc.tbool.TBoolInst;
import com.mobilitydb.jdbc.tbool.TBoolInstSet;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TBoolInstSetTest extends BaseIntegrationTest {

    @Test
    void testStringConstructor() throws Exception {
        String value = "{true@2001-01-01 08:00:00+03, false@2001-01-03 08:00:00+03}";
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
    void testStringsConstructor() throws Exception {
        String[] values = new String[] {"true@2001-01-01 08:00:00+03", "false@2001-01-03 08:00:00+03",
            "false@2001-01-03 14:00:00+03"};

        TBoolInstSet tBoolInstSet = new TBoolInstSet(values);
        TBool tBool = new TBool(tBoolInstSet);

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
    void testInstantsConstructor() throws Exception {
        TBoolInst[] values = new TBoolInst[] {new TBoolInst("true@2001-01-01 08:00:00+03"),
                new TBoolInst("true@2001-01-01 10:30:00+03"), new TBoolInst("false@2001-01-01 15:30:00+03")};

        TBoolInstSet tBoolInstSet = new TBoolInstSet(values);
        TBool tBool = new TBool(tBoolInstSet);

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
}
