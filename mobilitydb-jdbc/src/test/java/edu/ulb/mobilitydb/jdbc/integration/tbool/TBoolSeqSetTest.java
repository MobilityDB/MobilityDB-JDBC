package edu.ulb.mobilitydb.jdbc.integration.tbool;

import edu.ulb.mobilitydb.jdbc.integration.BaseIntegrationTest;
import edu.ulb.mobilitydb.jdbc.tbool.TBool;
import edu.ulb.mobilitydb.jdbc.tbool.TBoolSeq;
import edu.ulb.mobilitydb.jdbc.tbool.TBoolSeqSet;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TBoolSeqSetTest extends BaseIntegrationTest {
    @Test
    void testStringConstructor() throws Exception {
        String value = "{[True@2019-09-08 08:00:00+02 , False@2019-09-09 08:00:00+02, False@2019-09-10 08:00:00+02]," +
                "[False@2019-09-11 08:00:00+02, True@2019-09-12 08:00:00+02]}";
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
            fail("TInt was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testStringsConstructor() throws Exception {
        String[] values = new String[] {"[True@2019-09-08 08:00:00+02 , False@2019-09-09 08:00:00+02, " +
                "False@2019-09-10 08:00:00+02]", "[False@2019-09-11 08:00:00+02, True@2019-09-12 08:00:00+02]"};

        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(values);
        TBool tBool = new TBool(tBoolSeqSet);

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
            fail("TInt was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testSequencesConstructor() throws Exception {
        TBoolSeq[] values = new TBoolSeq[]{ new TBoolSeq("[True@2019-09-08 08:00:00+02 , " +
                "False@2019-09-09 08:00:00+02, False@2019-09-10 08:00:00+02]"),
                new TBoolSeq("[False@2019-09-11 08:00:00+02, True@2019-09-12 08:00:00+02]")};

        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(values);
        TBool tBool = new TBool(tBoolSeqSet);

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
            fail("TInt was not retrieved.");
        }

        readStatement.close();
    }
}
