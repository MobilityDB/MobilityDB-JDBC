package com.mobilitydb.jdbc.unit.tbool;

import com.mobilitydb.jdbc.tbool.TBoolInst;
import com.mobilitydb.jdbc.temporal.TemporalType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

class TBoolInstTest {
    @Test
    void testConstructor() throws SQLException {
        String value = "true@2019-09-08 06:04:32+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);

        TBoolInst tBoolInst = new TBoolInst(value);
        TBoolInst other = new TBoolInst(true, otherDate);

        assertEquals(tBoolInst.getTemporalValue(), other.getTemporalValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "true@2019-09-08 06:04:32+02",
        "false@2019-09-08 06:04:32+02"
    })
    void testInstSetType(String value) throws SQLException {
        TBoolInst tBoolInst = new TBoolInst(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT, tBoolInst.getTemporalType());
    }

    @Test
    void testInvalidEmptyValue() {
        SQLException thrown = assertThrows(
            SQLException.class,
            () -> {
                TBoolInst tBoolInst = new TBoolInst("");
            }
        );
        assertTrue(thrown.getMessage().contains("Value cannot be empty."));
    }
}

