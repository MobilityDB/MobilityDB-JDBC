package com.mobilitydb.jdbc.integration.tpoint.tgeog;

import com.mobilitydb.jdbc.integration.BaseIntegrationTest;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPoint;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointInst;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointInstSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TGeogPointInstSetTest extends BaseIntegrationTest {
    @ParameterizedTest
    @ValueSource(strings = {
            "{SRID=4326;Point(10 10)@2019-09-08 05:04:45+01, SRID=4326;Point(20 20)@2019-09-09 05:04:45+01, " +
                    "SRID=4326;Point(20 20)@2019-09-10 05:04:45+01}",
            "{Point(10 10)@2019-09-08 05:04:45+01, Point(20 20)@2019-09-09 05:04:45+01, " +
                    "Point(20 20)@2019-09-10 05:04:45+01}"
    })
    void testStringConstructor(String value) throws Exception {
        TGeogPoint tGeogPoint = new TGeogPoint(value);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tGeogpoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeogPoint);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tGeogpoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeogPoint);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeogPoint retrievedTGeogPoint = (TGeogPoint) rs.getObject(1);
            assertEquals(tGeogPoint.getTemporal(), retrievedTGeogPoint.getTemporal());
        } else {
            fail("TGeogPoint was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testStringsConstructor() throws Exception {
        String[] values = new String[] {
            "SRID=4326;Point(10 10)@2001-01-01 08:20:00+03",
            "Point(10 20)@2001-01-03 18:08:00+03", // Default SRID
            "SRID=4326;Point(20 20)@2001-01-03 20:20:00+03"
        };

        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(values);
        TGeogPoint tGeogPoint = new TGeogPoint(tGeogPointInstSet);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tGeogpoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeogPoint);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tGeogpoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeogPoint);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeogPoint retrievedTGeogPoint = (TGeogPoint) rs.getObject(1);
            assertEquals(tGeogPoint.getTemporal(), retrievedTGeogPoint.getTemporal());
        } else {
            fail("TGeogPoint was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testInstantsConstructor() throws Exception {
        TGeogPointInst[] values = new TGeogPointInst[] {
            new TGeogPointInst("SRID=4326;Point(10 10)@2001-01-01 08:30:00+02"),
            new TGeogPointInst("Point(10 20)@2001-01-03 18:00:00+02"), // Default SRID
            new TGeogPointInst("SRID=4326;Point(12 20)@2001-01-03 20:20:00+02")
        };

        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(values);
        TGeogPoint tGeogPoint = new TGeogPoint(tGeogPointInstSet);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tGeogpoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeogPoint);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tGeogpoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeogPoint);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeogPoint retrievedTGeogPoint = (TGeogPoint) rs.getObject(1);
            assertEquals(tGeogPoint.getTemporal(), retrievedTGeogPoint.getTemporal());
        } else {
            fail("TGeogPoint was not retrieved.");
        }

        readStatement.close();
    }
}