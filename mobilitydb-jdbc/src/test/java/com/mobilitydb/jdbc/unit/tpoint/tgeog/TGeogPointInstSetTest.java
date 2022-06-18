package com.mobilitydb.jdbc.unit.tpoint.tgeog;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointInst;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointInstSet;
import org.junit.jupiter.api.Test;
import org.postgis.Point;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TGeogPointInstSetTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "{Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02}";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);

        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(value);
        TGeogPointInst[] instants = new TGeogPointInst[]{
            new TGeogPointInst(new Point(0, 0), dateOne),
            new TGeogPointInst(new Point (1, 1), dateTwo)
        };
        TGeogPointInstSet otherTGeogPointInstSet = new TGeogPointInstSet(instants);

        assertEquals(tGeogPointInstSet.getValues(), otherTGeogPointInstSet.getValues());
        assertEquals(tGeogPointInstSet, otherTGeogPointInstSet);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "{Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02}";
        String secondValue = "{Point(0 1)@2001-01-01 08:00:00+02, Point(1 0)@2001-01-03 08:00:00+02}";
        String thirdValue = "{Point(1 0)@2001-01-01 08:00:00+02, " +
                "Point(0 2)@2001-01-03 08:00:00+02, " +
                "Point(3 0)@2001-01-04 08:00:00+02}";

        TGeogPointInstSet firstTGeogPointInstSet = new TGeogPointInstSet(firstValue);
        TGeogPointInstSet secondTGeogPointInstSet = new TGeogPointInstSet(secondValue);
        TGeogPointInstSet thirdTGeogPointInstSet = new TGeogPointInstSet(thirdValue);

        assertNotEquals(firstTGeogPointInstSet, secondTGeogPointInstSet);
        assertNotEquals(firstTGeogPointInstSet, thirdTGeogPointInstSet);
        assertNotEquals(secondTGeogPointInstSet, thirdTGeogPointInstSet);
        assertNotEquals(firstTGeogPointInstSet, new Object());
    }

    @Test
    void testStringArrayStringConstructors() throws SQLException {
        String value = "{Point(0 0)@2001-01-01 08:00:00+02, Point(2 2)@2001-01-03 08:00:00+02}";

        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(value);
        String[] instants = new String[]{
            "Point(0 0)@2001-01-01 08:00:00+02",
            "Point(2 2)@2001-01-03 08:00:00+02"
        };
        TGeogPointInstSet otherTGeogPointInstSet = new TGeogPointInstSet(instants);

        assertEquals(tGeogPointInstSet.getValues(), otherTGeogPointInstSet.getValues());
    }

    @Test
    void testInstSetType() throws SQLException {
        String value = "{Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02}";
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT_SET, tGeogPointInstSet.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        String value = String.format(
            "{SRID=4326;POINT(0 0)@2001-01-01 08:00:00%1$s, SRID=4326;POINT(1 1)@2001-01-03 08:00:00%1$s}",
            tz.toString().substring(0, 3)
        );
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(value);
        String newValue = tGeogPointInstSet.buildValue();
        assertEquals(value, newValue);
    }
}
