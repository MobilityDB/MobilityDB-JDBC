package edu.ulb.mobilitydb.jdbc.integration.tpoint.tgeom;

import edu.ulb.mobilitydb.jdbc.integration.BaseIntegrationTest;
import edu.ulb.mobilitydb.jdbc.tpoint.tgeom.TGeomPoint;
import edu.ulb.mobilitydb.jdbc.tpoint.tgeom.TGeomPointInst;
import edu.ulb.mobilitydb.jdbc.tpoint.tgeom.TGeomPointSeq;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TGeomPointSeqTest extends BaseIntegrationTest {

    @Test
    void testStringConstructor() throws Exception {
        String value = "[Point(10 10)@2019-09-08 09:07:32+04, Point(10 20)@2019-09-09 09:07:32+04," +
                "Point(20 20)@2019-09-10 09:07:32+04]";

        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(value);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tgeompoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeomPointSeq.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tgeompoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeomPointSeq.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeomPoint retrievedTGeomPoint = (TGeomPoint) rs.getObject(1);
            assertEquals(tGeomPointSeq, new TGeomPointSeq(retrievedTGeomPoint));
        } else {
            fail("TGeomPoint was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testStringsIncUppDefaultConstructor() throws Exception {
        String[] values = new String[] {"Point(10 10)@2019-09-08 09:07:32+04", "Point(10 20)@2019-09-09 09:07:32+04",
            "Point(20 20)@2019-09-10 09:07:32+04"};

        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(values);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tgeompoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeomPointSeq.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tgeompoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeomPointSeq.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeomPoint retrievedTGeomPoint = (TGeomPoint) rs.getObject(1);
            assertEquals(tGeomPointSeq, new TGeomPointSeq(retrievedTGeomPoint));
        } else {
            fail("TGeomPoint was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testStringsIncUppConstructor() throws Exception {
        String[] values = new String[] {"Point(10 10)@2019-09-08 09:07:32+04", "Point(10 20)@2019-09-09 09:07:32+04"};

        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(values, true, true);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tgeompoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeomPointSeq.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tgeompoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeomPointSeq.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeomPoint retrievedTGeomPoint = (TGeomPoint) rs.getObject(1);
            assertEquals(tGeomPointSeq, new TGeomPointSeq(retrievedTGeomPoint));
        } else {
            fail("TGeomPoint was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testInstantsIncUppDefaultConstructor() throws Exception {
        TGeomPointInst[] values = new TGeomPointInst[] {new TGeomPointInst("Point(10 10)@2019-09-08 09:07:32+04"),
                new TGeomPointInst("Point(10 20)@2019-09-09 09:07:32+04"),
                new TGeomPointInst("Point(20 20)@2019-09-10 09:07:32+04")};

        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(values);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tgeompoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeomPointSeq.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tgeompoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeomPointSeq.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeomPoint retrievedTGeomPoint = (TGeomPoint) rs.getObject(1);
            assertEquals(tGeomPointSeq, new TGeomPointSeq(retrievedTGeomPoint));
        } else {
            fail("TGeomPoint was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testInstantsIncUppConstructor() throws Exception {
        TGeomPointInst[] values = new TGeomPointInst[] {new TGeomPointInst("Point(10 10)@2001-01-01 08:30:00+02"),
                new TGeomPointInst("Point(10 20)@2001-01-03 18:00:00+02"),
                new TGeomPointInst("Point(12 20)@2001-01-03 20:20:00+02")};

        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(values, true, true);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tgeompoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeomPointSeq.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tgeompoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeomPointSeq.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeomPoint retrievedTGeomPoint = (TGeomPoint) rs.getObject(1);
            assertEquals(tGeomPointSeq, new TGeomPointSeq(retrievedTGeomPoint));
        } else {
            fail("TGeomPoint was not retrieved.");
        }

        readStatement.close();
    }

}
