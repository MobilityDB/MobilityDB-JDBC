package com.mobilitydb.jdbc.integration.tpoint.tgeog;

import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPoint;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointInst;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointSeq;
import com.mobilitydb.jdbc.integration.BaseIntegrationTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.fail;

class TGeogPointSeqTest extends BaseIntegrationTest {
    @ParameterizedTest
    @ValueSource(strings = {
        "[SRID=4326;Point(10 10)@2019-09-08 09:07:32+04, SRID=4326;Point(10 20)@2019-09-09 09:07:32+04," +
                "SRID=4326;Point(20 20)@2019-09-10 09:07:32+04]",
        "Interp=Stepwise;[SRID=4326;Point(10 10)@2019-09-08 09:07:32+04, " +
                "SRID=4326;Point(10 20)@2019-09-09 09:07:32+04," +
                "SRID=4326;Point(20 20)@2019-09-10 09:07:32+04]",
        "SRID=4326;[Point(10 10)@2019-09-08 09:07:32+04, Point(10 20)@2019-09-09 09:07:32+04," +
                "Point(20 20)@2019-09-10 09:07:32+04]",
        "SRID=4326;Interp=Stepwise;[Point(10 10)@2019-09-08 09:07:32+04, Point(10 20)@2019-09-09 09:07:32+04," +
                "Point(20 20)@2019-09-10 09:07:32+04]"
    })
    void testStringConstructor(String value) throws Exception {
        TGeogPoint tGeogPoint = new TGeogPoint(value);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tgeogpoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeogPoint);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tgeogpoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeogPoint);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeogPoint retrievedTGeogPoint = (TGeogPoint) rs.getObject(1);
            Assertions.assertEquals(tGeogPoint.getTemporal(), retrievedTGeogPoint.getTemporal());
        } else {
            fail("TGeogPoint was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testStringsIncUppDefaultConstructor() throws Exception {
        String[] values = new String[] {
            "SRID=4326;Point(10 10)@2019-09-08 09:07:32+04",
            "SRID=4326;Point(10 20)@2019-09-09 09:07:32+04",
            "SRID=4326;Point(20 20)@2019-09-10 09:07:32+04"
        };

        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(false, values);
        TGeogPoint tGeogPoint = new TGeogPoint(tGeogPointSeq);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tgeogpoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeogPoint);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tgeogpoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeogPoint);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeogPoint retrievedTGeogPoint = (TGeogPoint) rs.getObject(1);
            Assertions.assertEquals(tGeogPoint.getTemporal(), retrievedTGeogPoint.getTemporal());
        } else {
            fail("TGeogPoint was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testStringsIncUppConstructor() throws Exception {
        String[] values = new String[] {
            "SRID=4326;Point(10 10)@2019-09-08 09:07:32+04",
            "SRID=4326;Point(10 20)@2019-09-09 09:07:32+04"
        };

        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(false, values, true, true);
        TGeogPoint tGeogPoint = new TGeogPoint(tGeogPointSeq);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tgeogpoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeogPoint);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tgeogpoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeogPoint);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeogPoint retrievedTGeogPoint = (TGeogPoint) rs.getObject(1);
            Assertions.assertEquals(tGeogPoint.getTemporal(), retrievedTGeogPoint.getTemporal());
        } else {
            fail("TGeogPoint was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testInstantsIncUppDefaultConstructor() throws Exception {
        TGeogPointInst[] values = new TGeogPointInst[] {
            new TGeogPointInst("SRID=4326;Point(10 10)@2019-09-08 09:07:32+04"),
            new TGeogPointInst("SRID=4326;Point(10 20)@2019-09-09 09:07:32+04"),
            new TGeogPointInst("SRID=4326;Point(20 20)@2019-09-10 09:07:32+04")
        };

        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(false, values);
        TGeogPoint tGeogPoint = new TGeogPoint(tGeogPointSeq);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tgeogpoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeogPoint);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tgeogpoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeogPoint);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeogPoint retrievedTGeogPoint = (TGeogPoint) rs.getObject(1);
            Assertions.assertEquals(tGeogPoint.getTemporal(), retrievedTGeogPoint.getTemporal());
        } else {
            fail("TGeogPoint was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testInstantsIncUppConstructor() throws Exception {
        TGeogPointInst[] values = new TGeogPointInst[] {
            new TGeogPointInst("SRID=4326;Point(10 10)@2001-01-01 08:30:00+02"),
            new TGeogPointInst("SRID=4326;Point(10 20)@2001-01-03 18:00:00+02"),
            new TGeogPointInst("SRID=4326;Point(12 20)@2001-01-03 20:20:00+02")
        };

        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(false, values, true, true);
        TGeogPoint tGeogPoint = new TGeogPoint(tGeogPointSeq);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tgeogpoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeogPoint);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tgeogpoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeogPoint);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeogPoint retrievedTGeogPoint = (TGeogPoint) rs.getObject(1);
            Assertions.assertEquals(tGeogPoint.getTemporal(), retrievedTGeogPoint.getTemporal());
        } else {
            fail("TGeogPoint was not retrieved.");
        }

        readStatement.close();
    }
}
