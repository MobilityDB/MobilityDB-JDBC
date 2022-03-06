package edu.ulb.mobilitydb.jdbc.integration.tint;

import edu.ulb.mobilitydb.jdbc.integration.BaseIntegrationTest;
import edu.ulb.mobilitydb.jdbc.tint.TInt;
import edu.ulb.mobilitydb.jdbc.tint.TIntInst;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TIntInstTest extends BaseIntegrationTest {

    @Test
    void testIntTimeConstructor() throws Exception {
        String value = "10@2021-04-08 05:04:45+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime time = OffsetDateTime.of(2021,4, 8,
                5, 4, 45, 0, tz);
        TIntInst tIntInst = new TIntInst(10, time);
        TInt tInt = new TInt(tIntInst);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tInt);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tint WHERE temporaltype=?;");
        readStatement.setObject(1, tInt);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TInt retrievedTInt = (TInt) rs.getObject(1);
            assertEquals(tInt.getTemporal(), retrievedTInt.getTemporal());
        } else {
            fail("TInt was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testStringConstructor() throws Exception {
        String value = "10@2021-04-08 05:04:45+01";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime time = OffsetDateTime.of(2021,4, 8,
                5, 4, 45, 0, tz);
        TIntInst tIntInst = new TIntInst(value);

        PreparedStatement insertStatement = con.prepareStatement("INSERT INTO tbl_tint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tIntInst.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement("SELECT temporaltype FROM tbl_tint WHERE temporaltype=?;");
        readStatement.setObject(1, tIntInst.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TInt retrievedTInt = (TInt) rs.getObject(1);
            assertEquals(tIntInst, new TIntInst(retrievedTInt));
        } else {
            fail("TInt was not retrieved.");
        }

        readStatement.close();
    }
}
