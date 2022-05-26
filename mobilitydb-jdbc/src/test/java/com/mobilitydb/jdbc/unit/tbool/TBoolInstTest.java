package com.mobilitydb.jdbc.unit.tbool;

import com.mobilitydb.jdbc.tbool.TBoolInst;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TBoolInstTest {
    @Test
    void testConstructor() throws SQLException {
        String value = "true@2019-09-08 06:04:32+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);

        TBoolInst tBoolInst = new TBoolInst(value);
        TBoolInst other = new TBoolInst(true, otherDate);

        assertEquals(other.getTemporalValue(), tBoolInst.getTemporalValue());
    }
}

