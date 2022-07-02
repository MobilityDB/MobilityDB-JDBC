package com.mobilitydb.jdbc.unit.tint;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tint.TIntInst;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TIntInstTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "10@2019-09-08 06:04:32+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);

        TIntInst tIntInst = new TIntInst(value);
        TIntInst other = new TIntInst(10, otherDate);

        assertEquals(other.getTemporalValue(), tIntInst.getTemporalValue());
        assertEquals(other, tIntInst);
    }

    @Test
    void testEquals() throws SQLException {
        String value = "10@2019-09-08 06:04:32+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        String otherValue = "12@2019-09-09 06:04:32+02";

        TIntInst first = new TIntInst(value);
        TIntInst second = new TIntInst(10, otherDate);
        TIntInst third = new TIntInst(otherValue);

        assertEquals(first, second);
        assertNotEquals(first, third);
        assertNotEquals(second, third);
        assertNotEquals(first, new Object());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "254@2019-09-08 06:04:32+02",
            "659@2019-09-08 06:04:32+02"
    })
    void testInstSetType(String value) throws SQLException {
        TIntInst tIntInst = new TIntInst(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT, tIntInst.getTemporalType());
    }

    @Test
    void testInvalidEmptyValue() {
        SQLException thrown = assertThrows(
            SQLException.class,
            () -> {
                TIntInst tIntInst = new TIntInst("");
            }
        );
        assertTrue(thrown.getMessage().contains("Value cannot be empty."));
    }

    @Test
    void testBuildValue() throws SQLException {
        String value = "10@2019-09-08 06:04:32";
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        value = value + tz.toString().substring(0, 3);
        TIntInst tIntInst = new TIntInst(value);
        String newValue = tIntInst.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        String value = "10@2019-09-08 06:04:32+02";
        TIntInst tIntInst = new TIntInst(value);
        List<Integer> values = tIntInst.getValues();
        assertEquals(1, values.size());
        assertEquals(10, values.get(0));
    }
}
