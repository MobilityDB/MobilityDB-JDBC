package edu.ulb.mobilitydb.jdbc.integration.ttext;

import edu.ulb.mobilitydb.jdbc.integration.BaseIntegrationTest;
import edu.ulb.mobilitydb.jdbc.ttext.TText;
import edu.ulb.mobilitydb.jdbc.ttext.TTextInst;
import edu.ulb.mobilitydb.jdbc.ttext.TTextSeq;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TTextSeqTest extends BaseIntegrationTest {

    @Test
    void testStringConstructor() throws Exception {
        String value = "[A@2001-01-01 08:00:00+02, B@2001-01-03 08:00:00+02]";

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

    @Test
    void testStringsIncUppDefaultConstructor() throws Exception {
        String[] values = new String[] {"A@2001-01-01 08:00:00+02", "A@2001-01-03 08:00:00+02"};

        TTextSeq tTextSeq = new TTextSeq(values);
        TText tText = new TText(tTextSeq);

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
    void testStringsIncUppConstructor() throws Exception {
        String[] values = new String[] {"A@2001-01-01 08:00:00+02", "A@2001-01-03 08:00:00+02"};

        TTextSeq tTextSeq = new TTextSeq(values, true, true);
        TText tText = new TText(tTextSeq);

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
    void testInstantsIncUppDefaultConstructor() throws Exception {
        TTextInst[] values = new TTextInst[] {new TTextInst("A@2001-01-01 08:30:00+02"),
                new TTextInst("D@2001-01-03 18:00:00+02"), new TTextInst("D@2001-01-03 20:20:00+02")};

        TTextSeq tTextSeq = new TTextSeq(values);
        TText tText = new TText(tTextSeq);

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
    void testInstantsIncUppConstructor() throws Exception {
        TTextInst[] values = new TTextInst[] {new TTextInst("A@2001-01-01 08:30:00+02"),
                new TTextInst("B@2001-01-03 18:00:00+02"), new TTextInst("B@2001-01-03 20:20:00+02")};

        TTextSeq tTextSeq = new TTextSeq(values, true, false);
        TText tText = new TText(tTextSeq);

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
