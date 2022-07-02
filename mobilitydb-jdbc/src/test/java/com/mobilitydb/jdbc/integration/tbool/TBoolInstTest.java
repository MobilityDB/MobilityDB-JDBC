package com.mobilitydb.jdbc.integration.tbool;

import com.mobilitydb.jdbc.integration.BaseIntegrationTest;
import com.mobilitydb.jdbc.tbool.TBool;
import com.mobilitydb.jdbc.tbool.TBoolInst;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TBoolInstTest extends BaseIntegrationTest {
    @Test
    void testBoolConstructor() throws Exception {
        ZoneOffset tz = ZoneOffset.of("+04:00");
        OffsetDateTime time = OffsetDateTime.of(2021,4, 8,
                5, 8, 31, 0, tz);
        TBoolInst tBoolInst = new TBoolInst(true, time);
        TBool tBool = new TBool(tBoolInst);

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
    void testStringConstructor() throws Exception {
        String value = "false@2021-04-08 05:04:45+01";
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
}
