package com.mobilitydb.jdbc.integration.tint;

import com.mobilitydb.jdbc.integration.BaseIntegrationTest;
import com.mobilitydb.jdbc.tint.TInt;
import com.mobilitydb.jdbc.tint.TIntInst;
import com.mobilitydb.jdbc.tint.TIntSeq;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TIntSeqTest extends BaseIntegrationTest {
    @Test
    void testStringConstructor() throws Exception {
        String value = "[1@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02]";

        TInt tInt = new TInt(value);

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
    void testStringsIncUppDefaultConstructor() throws Exception {
        String[] values = new String[] {"1@2001-01-01 08:00:00+02", "1@2001-01-03 08:00:00+02"};

        TIntSeq tIntSeq = new TIntSeq(values);
        TInt tInt = new TInt(tIntSeq);

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
    void testStringsIncUppConstructor() throws Exception {
        String[] values = new String[] {"1@2001-01-01 08:00:00+02", "1@2001-01-03 08:00:00+02"};

        TIntSeq tIntSeq = new TIntSeq(values, true, true);
        TInt tInt = new TInt(tIntSeq);

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
    void testInstantsIncUppDefaultConstructor() throws Exception {
        TIntInst[] values = new TIntInst[] {new TIntInst("25@2001-01-01 08:30:00+02"),
                new TIntInst("76@2001-01-03 18:00:00+02"), new TIntInst("76@2001-01-03 20:20:00+02")};
        //[25@2001-01-01 08:30:00+02, 48@2001-01-03 18:00:00+02, 76@2001-01-03 20:20:00+02]
        TIntSeq tIntSeq = new TIntSeq(values);
        TInt tInt = new TInt(tIntSeq);

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
    void testInstantsIncUppConstructor() throws Exception {
        TIntInst[] values = new TIntInst[] {new TIntInst("25@2001-01-01 08:30:00+02"),
                new TIntInst("76@2001-01-03 18:00:00+02"), new TIntInst("76@2001-01-03 20:20:00+02")};

        TIntSeq tIntSeq = new TIntSeq(values, true, false);
        TInt tInt = new TInt(tIntSeq);

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
}
