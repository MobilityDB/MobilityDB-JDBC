package edu.ulb.mobilitydb.jdbc.integration.ttext;

import edu.ulb.mobilitydb.jdbc.integration.BaseIntegrationTest;

import edu.ulb.mobilitydb.jdbc.ttext.TText;
import edu.ulb.mobilitydb.jdbc.ttext.TTextInst;
import edu.ulb.mobilitydb.jdbc.ttext.TTextInstSet;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TTextInstSetTest extends BaseIntegrationTest {

    @Test
    void testStringConstructor() throws Exception {
        String value = "{A@2001-01-01 08:00:00+02, B@2001-01-03 08:00:00+02}";

        TTextInstSet tTextInstSet = new TTextInstSet(value);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_ttext (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tTextInstSet.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_ttext WHERE temporaltype=?;");
        readStatement.setObject(1, tTextInstSet.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TText retrievedTText = (TText) rs.getObject(1);
            assertEquals(tTextInstSet, new TTextInstSet(retrievedTText));
        } else {
            fail("TText was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testStringsConstructor() throws Exception {
        String[] values = new String[] {"A@2001-01-01 08:30:00+02", "B@2001-01-03 18:00:00+02",
                "C@2001-01-03 20:20:00+02"};

        TTextInstSet tTextInstSet = new TTextInstSet(values);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_ttext (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tTextInstSet.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_ttext WHERE temporaltype=?;");
        readStatement.setObject(1, tTextInstSet.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TText retrievedTText = (TText) rs.getObject(1);
            assertEquals(tTextInstSet, new TTextInstSet(retrievedTText));
        } else {
            fail("TText was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testInstantsConstructor() throws Exception {
        TTextInst[] values = new TTextInst[] {new TTextInst("A@2001-01-01 08:30:00+02"),
                new TTextInst("B@2001-01-03 18:00:00+02"), new TTextInst("C@2001-01-03 20:20:00+02")};

        TTextInstSet tTextInstSet = new TTextInstSet(values);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_ttext (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tTextInstSet.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_ttext WHERE temporaltype=?;");
        readStatement.setObject(1, tTextInstSet.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TText retrievedTText = (TText) rs.getObject(1);
            assertEquals(tTextInstSet, new TTextInstSet(retrievedTText));
        } else {
            fail("TText was not retrieved.");
        }

        readStatement.close();
    }
}
