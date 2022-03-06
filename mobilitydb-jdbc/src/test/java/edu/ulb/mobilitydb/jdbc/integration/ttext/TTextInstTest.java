package edu.ulb.mobilitydb.jdbc.integration.ttext;

import edu.ulb.mobilitydb.jdbc.integration.BaseIntegrationTest;
import edu.ulb.mobilitydb.jdbc.ttext.TText;
import edu.ulb.mobilitydb.jdbc.ttext.TTextInst;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TTextInstTest extends BaseIntegrationTest {
    @Test
    void testStringTimeConstructor() throws Exception {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime time = OffsetDateTime.of(2021,4, 8,
                5, 4, 45, 0, tz);
        TTextInst tTextInst = new TTextInst("A", time);
        TText tText = new TText(tTextInst);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_ttext (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tText);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_ttext WHERE temporaltype=?;");
        readStatement.setObject(1, tText);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TText retrievedTText = (TText) rs.getObject(1);
            assertEquals(tText.getTemporal(), retrievedTText.getTemporal());
        } else {
            fail("TText was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testStringConstructor() throws Exception {
        String value = "A@2021-04-08 05:04:45+02";
        
        TText tText = new TText(value);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_ttext (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tText);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_ttext WHERE temporaltype=?;");
        readStatement.setObject(1, tText);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TText retrievedTText = (TText) rs.getObject(1);
            assertEquals(tText.getTemporal(), retrievedTText.getTemporal());
        } else {
            fail("TText was not retrieved.");
        }

        readStatement.close();
    }
}
