package com.mobilitydb.jdbc.unit.tpoint.tgeog;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointInst;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgis.Point;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

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

    @ParameterizedTest
    @ValueSource(strings = {
            "SRID=4326;Point(10.0 10.0)@2021-04-08 05:04:45+01",
            "Point(1 0)@2017-01-01 08:00:05+02",
            "SRID=4326;010100000000000000000000000000000000000000@2021-04-08 05:04:45+01",
            "010100000000000000000000000000000000000000@2021-04-08 05:04:45+01"
    })
    void testInstSetType(String value) throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT, tGeogPointInst.getTemporalType());
    }

    @Test
    void testInvalidEmptyValue() {
        SQLException thrown = assertThrows(
            SQLException.class,
            () -> {
                TGeogPointInst tGeogPointInst = new TGeogPointInst("");
            }
        );
        assertTrue(thrown.getMessage().contains("Value cannot be empty."));
    }
}
