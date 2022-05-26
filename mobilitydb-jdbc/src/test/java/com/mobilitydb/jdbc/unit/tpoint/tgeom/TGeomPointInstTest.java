package com.mobilitydb.jdbc.unit.tpoint.tgeom;

import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointInst;
import org.junit.jupiter.api.Test;
import org.postgis.Point;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TGeomPointInstTest {
    @Test
    void testConstructor() throws SQLException {
        String value = "Point(0 0)@2017-01-01 08:00:05+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2017,1, 1,
                8, 0, 5, 0, tz);

        TGeomPointInst tGeomPointInst = new TGeomPointInst(value);
        TGeomPointInst other = new TGeomPointInst(new Point(0 ,0), otherDate);

        assertEquals(other.getTemporalValue(), tGeomPointInst.getTemporalValue());
    }
}
