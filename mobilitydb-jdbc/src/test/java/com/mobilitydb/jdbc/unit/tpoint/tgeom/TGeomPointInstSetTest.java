package com.mobilitydb.jdbc.unit.tpoint.tgeom;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointInst;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointInstSet;
import org.junit.jupiter.api.Test;
import org.postgis.Point;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TGeomPointInstSetTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "{Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02}";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);

        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(value);
        TGeomPointInst[] instants = new TGeomPointInst[]{
            new TGeomPointInst(new Point(0, 0), dateOne),
            new TGeomPointInst(new Point (1, 1), dateTwo)
        };
        TGeomPointInstSet otherTGeomPointInstSet = new TGeomPointInstSet(instants);

        assertEquals(tGeomPointInstSet.getValues(), otherTGeomPointInstSet.getValues());
        assertEquals(tGeomPointInstSet, otherTGeomPointInstSet);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "{Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02}";
        String secondValue = "{Point(0 1)@2001-01-01 08:00:00+02, Point(1 0)@2001-01-03 08:00:00+02}";
        String thirdValue = "{Point(1 0)@2001-01-01 08:00:00+02, " +
                "Point(0 2)@2001-01-03 08:00:00+02, " +
                "Point(3 0)@2001-01-04 08:00:00+02}";

        TGeomPointInstSet firstTGeomPointInstSet = new TGeomPointInstSet(firstValue);
        TGeomPointInstSet secondTGeomPointInstSet = new TGeomPointInstSet(secondValue);
        TGeomPointInstSet thirdTGeomPointInstSet = new TGeomPointInstSet(thirdValue);

        assertNotEquals(firstTGeomPointInstSet, secondTGeomPointInstSet);
        assertNotEquals(firstTGeomPointInstSet, thirdTGeomPointInstSet);
        assertNotEquals(secondTGeomPointInstSet, thirdTGeomPointInstSet);
        assertNotEquals(firstTGeomPointInstSet, new Object());
    }

    @Test
    void testStringArrayStringConstructors() throws SQLException {
        String value = "{Point(0 0)@2001-01-01 08:00:00+02, Point(2 2)@2001-01-03 08:00:00+02}";

        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(value);
        String[] instants = new String[]{
            "Point(0 0)@2001-01-01 08:00:00+02",
            "Point(2 2)@2001-01-03 08:00:00+02"
        };
        TGeomPointInstSet otherTGeomPointInstSet = new TGeomPointInstSet(instants);

        assertEquals(tGeomPointInstSet.getValues(), otherTGeomPointInstSet.getValues());
    }

    @Test
    void testInstSetType() throws SQLException {
        String value = "{Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02}";
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT_SET, tGeomPointInstSet.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        String value = String.format(
            "{SRID=4326;POINT(0 0)@2001-01-01 08:00:00%1$s, SRID=4326;POINT(1 1)@2001-01-03 08:00:00%1$s}",
            tz.toString().substring(0, 3)
        );
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(value);
        String newValue = tGeomPointInstSet.buildValue();
        assertEquals(value, newValue);
    }
}
