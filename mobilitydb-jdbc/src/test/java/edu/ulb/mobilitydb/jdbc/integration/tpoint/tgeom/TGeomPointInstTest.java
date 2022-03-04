package edu.ulb.mobilitydb.jdbc.integration.tpoint.tgeom;

import edu.ulb.mobilitydb.jdbc.integration.BaseIntegrationTest;
import edu.ulb.mobilitydb.jdbc.tpoint.tgeom.TGeomPoint;
import edu.ulb.mobilitydb.jdbc.tpoint.tgeom.TGeomPointInst;
import org.junit.jupiter.api.Test;
import org.postgis.Point;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TGeomPointInstTest extends BaseIntegrationTest {

    @Test
    void testIntConstructor() throws Exception {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime time = OffsetDateTime.of(2021,4, 8,
                5, 4, 45, 0, tz);
        Point p = new Point(2, 3);
        TGeomPointInst tGeomPointInst = new TGeomPointInst(p, time);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tgeompoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeomPointInst.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tgeompoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeomPointInst.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeomPoint retrievedTGeomPoint = (TGeomPoint) rs.getObject(1);
            assertEquals(tGeomPointInst, new TGeomPointInst(retrievedTGeomPoint));
        } else {
            fail("TGeomPoint was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testStringBinaryConstructor() throws Exception {
        String value = "010100000000000000000000000000000000000000@2021-04-08 05:04:45+01";
        TGeomPointInst tGeomPointInst = new TGeomPointInst(value);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tgeompoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeomPointInst.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tgeompoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeomPointInst.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeomPoint retrievedTGeomPoint = (TGeomPoint) rs.getObject(1);
            assertEquals(tGeomPointInst, new TGeomPointInst(retrievedTGeomPoint));
        } else {
            fail("TGeomPoint was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testStringConstructor() throws Exception {
        String value = "SRID=4326;Point(10.0 10.0)@2021-04-08 05:04:45+01";
        TGeomPointInst tGeomPointInst = new TGeomPointInst(value);
        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tgeompoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeomPointInst.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tgeompoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeomPointInst.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeomPoint retrievedTGeomPoint = (TGeomPoint) rs.getObject(1);
            assertEquals(tGeomPointInst, new TGeomPointInst(retrievedTGeomPoint));
        } else {
            fail("TGeomPoint was not retrieved.");
        }

        readStatement.close();
    }
}
