package com.mobilitydb.jdbc.integration.tint;

import com.mobilitydb.jdbc.integration.BaseIntegrationTest;
import com.mobilitydb.jdbc.tint.TInt;
import com.mobilitydb.jdbc.tint.TIntInst;
import com.mobilitydb.jdbc.tint.TIntInstSet;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TIntInstSetTest extends BaseIntegrationTest {

    @Test
    void testStringConstructor() throws Exception {
        String value = "{1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02}";

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
    void testStringsConstructor() throws Exception {
        String[] values = new String[] {"25@2001-01-01 08:30:00+02", "48@2001-01-03 18:00:00+02",
                "76@2001-01-03 20:20:00+02"};

        TIntInstSet tIntInstSet = new TIntInstSet(values);
        TInt tInt = new TInt(tIntInstSet);

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
            assertEquals(tInt, retrievedTInt);
        } else {
            fail("TInt was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testInstantsConstructor() throws Exception {
        TIntInst[] values = new TIntInst[] {new TIntInst("25@2001-01-01 08:30:00+02"),
                new TIntInst("48@2001-01-03 18:00:00+02"), new TIntInst("76@2001-01-03 20:20:00+02")};

        TIntInstSet tIntInstSet = new TIntInstSet(values);
        TInt tInt = new TInt(tIntInstSet);

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
