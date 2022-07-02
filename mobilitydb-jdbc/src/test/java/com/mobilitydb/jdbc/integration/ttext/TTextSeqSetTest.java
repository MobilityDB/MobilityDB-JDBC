package com.mobilitydb.jdbc.integration.ttext;

import com.mobilitydb.jdbc.ttext.TTextSeqSet;
import com.mobilitydb.jdbc.integration.BaseIntegrationTest;

import com.mobilitydb.jdbc.ttext.TText;
import com.mobilitydb.jdbc.ttext.TTextSeq;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TTextSeqSetTest extends BaseIntegrationTest {

    @Test
    void testStringConstructor() throws Exception {
        String value = "{[A@2001-01-01 08:00:00+02, A@2001-01-03 08:00:00+02), " +
                "[B@2001-01-04 08:00:00+02, C@2001-01-05 08:00:00+02, C@2001-01-06 08:00:00+02]}";

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
    void testStringsConstructor() throws Exception {
        String[] values = new String[] {"[A@2001-01-01 08:00:00+02, A@2001-01-03 08:00:00+02)",
                "[B@2001-01-04 08:00:00+02, C@2001-01-05 08:00:00+02, C@2001-01-06 08:00:00+02]"};

        TTextSeqSet tTextSeqSet = new TTextSeqSet(values);
        TText tText = new TText(tTextSeqSet);

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
    void testSequencesConstructor() throws Exception {
        TTextSeq[] values = new TTextSeq[]{ new TTextSeq("[A@2001-01-01 08:00:00+02, A@2001-01-03 08:00:00+02)"),
                new TTextSeq("[F@2001-01-04 08:00:00+02, E@2001-01-05 08:00:00+02, E@2001-01-06 08:00:00+02]")};

        TTextSeqSet tTextSeqSet = new TTextSeqSet(values);
        TText tText = new TText(tTextSeqSet);

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
