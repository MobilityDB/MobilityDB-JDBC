package com.mobilitydb.jdbc.unit.tfloat;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tfloat.TFloatInst;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

class TFloatInstTest {
    @Test
    void testConstructor() throws SQLException {
        String value = "2.5@2019-09-08 06:04:32+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);

        TFloatInst tFloatInst = new TFloatInst(value);
        TFloatInst other = new TFloatInst(2.5f, otherDate);

        assertEquals(tFloatInst.getTemporalValue(), other.getTemporalValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "0.8@2019-09-08 06:04:32+02",
            "25.89@2019-09-08 06:04:32+02"
    })
    void testInstSetType(String value) throws SQLException {
        TFloatInst tFloatInst = new TFloatInst(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT, tFloatInst.getTemporalType());
    }

    @Test
    void testInvalidEmptyValue() {
        SQLException thrown = assertThrows(
            SQLException.class,
            () -> {
                TFloatInst tFloatInst = new TFloatInst("");
            }
        );
        assertTrue(thrown.getMessage().contains("Value cannot be empty."));
    }
}
