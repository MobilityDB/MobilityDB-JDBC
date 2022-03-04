package edu.ulb.mobilitydb.jdbc.integration.tpoint.tgeom;

import edu.ulb.mobilitydb.jdbc.integration.BaseIntegrationTest;
import edu.ulb.mobilitydb.jdbc.tpoint.tgeom.TGeomPoint;
import edu.ulb.mobilitydb.jdbc.tpoint.tgeom.TGeomPointInst;
import edu.ulb.mobilitydb.jdbc.tpoint.tgeom.TGeomPointInstSet;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TGeomPointInstSetTest extends BaseIntegrationTest {
    @Test
    void testStringConstructor() throws Exception {
        String value = "{Point(10 10)@2019-09-08 05:04:45+01, Point(20 20)@2019-09-09 05:04:45+01, " +
                "Point(20 20)@2019-09-10 05:04:45+01}";

        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(value);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tgeompoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeomPointInstSet.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tgeompoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeomPointInstSet.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeomPoint retrievedTGeomPoint = (TGeomPoint) rs.getObject(1);
            assertEquals(tGeomPointInstSet, new TGeomPointInstSet(retrievedTGeomPoint));
        } else {
            fail("TGeomPoint was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testStringsConstructor() throws Exception {
        String[] values = new String[] {"Point(10 10)@2001-01-01 08:20:00+03", "Point(10 20)@2001-01-03 18:08:00+03",
                "Point(20 20)@2001-01-03 20:20:00+03"};

        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(values);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tgeompoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeomPointInstSet.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tgeompoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeomPointInstSet.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeomPoint retrievedTGeomPoint = (TGeomPoint) rs.getObject(1);
            assertEquals(tGeomPointInstSet, new TGeomPointInstSet(retrievedTGeomPoint));
        } else {
            fail("TGeomPoint was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testInstantsConstructor() throws Exception {
        TGeomPointInst[] values = new TGeomPointInst[] {new TGeomPointInst("Point(10 10)@2001-01-01 08:30:00+02"),
                new TGeomPointInst("Point(10 20)@2001-01-03 18:00:00+02"),
                new TGeomPointInst("Point(12 20)@2001-01-03 20:20:00+02")};

        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(values);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tgeompoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeomPointInstSet.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tgeompoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeomPointInstSet.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeomPoint retrievedTGeomPoint = (TGeomPoint) rs.getObject(1);
            assertEquals(tGeomPointInstSet, new TGeomPointInstSet(retrievedTGeomPoint));
        } else {
            fail("TGeomPoint was not retrieved.");
        }

        readStatement.close();
    }
}
