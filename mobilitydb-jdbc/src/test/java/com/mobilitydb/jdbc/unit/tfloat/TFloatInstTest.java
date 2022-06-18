package com.mobilitydb.jdbc.unit.tfloat;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tfloat.TFloatInst;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

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
        assertEquals(other, tFloatInst);
    }

    @Test
    void testEquals() throws SQLException {
        String value = "10.67@2019-09-08 06:04:32+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        String otherValue = "12.77@2019-09-09 06:04:32+02";

        TFloatInst first = new TFloatInst(value);
        TFloatInst second = new TFloatInst(10.67f, otherDate);
        TFloatInst third = new TFloatInst(otherValue);

        assertEquals(first, second);
        assertNotEquals(first, third);
        assertNotEquals(second, third);
        assertNotEquals(first, new Object());
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

    @Test
    void testBuildValue() throws SQLException {
        String value = "1.8@2019-09-08 06:04:32";
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        value = value + tz.toString().substring(0, 3);
        TFloatInst tFloatInst = new TFloatInst(value);
        String newValue = tFloatInst.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        String value = "25.74@2019-09-08 06:04:32+02";
        TFloatInst tFloatInst = new TFloatInst(value);
        List<Float> values = tFloatInst.getValues();
        assertEquals(1, values.size());
        assertEquals(25.74f, values.get(0));
    }
}
