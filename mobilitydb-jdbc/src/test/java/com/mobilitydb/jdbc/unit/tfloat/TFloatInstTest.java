package com.mobilitydb.jdbc.unit.tfloat;

import com.mobilitydb.jdbc.tfloat.TFloatInst;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TFloatInstTest {
    @Test
    void testConstructor() throws SQLException {
        String value = "2.5@2019-09-08 06:04:32+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);

        TFloatInst tFloatInst = new TFloatInst(value);
        TFloatInst other = new TFloatInst(2.5f, otherDate);

        assertEquals(other.getTemporalValue(), tFloatInst.getTemporalValue());
    }
}
