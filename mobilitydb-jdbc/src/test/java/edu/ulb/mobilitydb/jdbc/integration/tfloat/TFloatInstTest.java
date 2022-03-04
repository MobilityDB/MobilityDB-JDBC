package edu.ulb.mobilitydb.jdbc.integration.tfloat;

import edu.ulb.mobilitydb.jdbc.integration.BaseIntegrationTest;
import edu.ulb.mobilitydb.jdbc.tfloat.TFloat;
import edu.ulb.mobilitydb.jdbc.tfloat.TFloatInst;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TFloatInstTest extends BaseIntegrationTest {
    @Test
    void testIntConstructor() throws Exception {
        String value = "1.5@2021-04-08 05:04:45+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime time = OffsetDateTime.of(2021,4, 8,
                5, 4, 45, 0, tz);
        TFloatInst tFloatInst = new TFloatInst(1.5f, time);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tfloat (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tFloatInst.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tfloat WHERE temporaltype=?;");
        readStatement.setObject(1, tFloatInst.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TFloat retrievedTFloat = (TFloat) rs.getObject(1);
            assertEquals(tFloatInst, new TFloatInst(retrievedTFloat));
        } else {
            fail("TFloat was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testStringConstructor() throws Exception {
        String value = "38.2@2021-04-08 05:04:45+01";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime time = OffsetDateTime.of(2021,4, 8,
                5, 4, 45, 0, tz);
        TFloatInst tFloatInst = new TFloatInst(value);

        PreparedStatement insertStatement = con.prepareStatement("INSERT INTO tbl_tfloat (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tFloatInst.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement("SELECT temporaltype FROM tbl_tfloat WHERE temporaltype=?;");
        readStatement.setObject(1, tFloatInst.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TFloat retrievedTFloat = (TFloat) rs.getObject(1);
            assertEquals(tFloatInst, new TFloatInst(retrievedTFloat));
        } else {
            fail("TFloat was not retrieved.");
        }

        readStatement.close();
    }
}
