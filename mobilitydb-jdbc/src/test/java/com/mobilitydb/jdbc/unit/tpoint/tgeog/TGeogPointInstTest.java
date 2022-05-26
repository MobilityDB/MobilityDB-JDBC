package com.mobilitydb.jdbc.unit.tpoint.tgeog;

import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointInst;
import org.junit.jupiter.api.Test;
import org.postgis.Point;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TGeogPointInstTest {
    @Test
    void testConstructor() throws SQLException {
        String value = "Point(0 0)@2017-01-01 08:00:05+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2017,1, 1,
                8, 0, 5, 0, tz);

        TGeogPointInst tGeogPointInst = new TGeogPointInst(value);
        TGeogPointInst other = new TGeogPointInst(new Point(0 ,0), otherDate);

        assertEquals(other.getTemporalValue(), tGeogPointInst.getTemporalValue());
    }
}
