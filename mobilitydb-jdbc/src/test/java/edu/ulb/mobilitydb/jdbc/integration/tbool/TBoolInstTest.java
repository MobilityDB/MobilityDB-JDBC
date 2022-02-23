package edu.ulb.mobilitydb.jdbc.integration.tbool;

import edu.ulb.mobilitydb.jdbc.integration.BaseIntegrationTest;
import edu.ulb.mobilitydb.jdbc.tbool.TBool;
import edu.ulb.mobilitydb.jdbc.tbool.TBoolInst;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class TBoolInstTest extends BaseIntegrationTest {
    @Test
    void testBoolConstructor() throws Exception {
        String value = "true@2021-04-08 05:08:31+04";
        ZoneOffset tz = ZoneOffset.of("+04:00");
        OffsetDateTime time = OffsetDateTime.of(2021,4, 8,
                5, 8, 31, 0, tz);
        TBoolInst tBoolInst = new TBoolInst(true, time);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tbool (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tBoolInst.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tbool WHERE temporaltype=?;");
        readStatement.setObject(1, tBoolInst.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TBool retrievedTBool = (TBool) rs.getObject(1);
            assertEquals(tBoolInst, new TBoolInst(retrievedTBool));
        } else {
            fail("TBool was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testStringConstructor() throws Exception {
        String value = "false@2021-04-08 05:04:45+01";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime time = OffsetDateTime.of(2021,4, 8,
                5, 4, 45, 0, tz);
        TBoolInst tBoolInst = new TBoolInst(value);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tbool (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tBoolInst.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tbool WHERE temporaltype=?;");
        readStatement.setObject(1, tBoolInst.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TBool retrievedTBool = (TBool) rs.getObject(1);
            assertEquals(tBoolInst, new TBoolInst(retrievedTBool));
        } else {
            fail("TBool was not retrieved.");
        }

        readStatement.close();
    }
}
