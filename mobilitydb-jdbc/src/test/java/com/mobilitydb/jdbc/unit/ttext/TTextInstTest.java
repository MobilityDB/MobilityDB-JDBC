package com.mobilitydb.jdbc.unit.ttext;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.ttext.TTextInst;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

class TTextInstTest {
    @Test
    void testConstructor() throws SQLException {
        String value = "abccccd@2019-09-08 06:04:32+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);

        TTextInst tTextInst = new TTextInst(value);
        TTextInst other = new TTextInst("abccccd", otherDate);

        assertEquals(other.getTemporalValue(), tTextInst.getTemporalValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "abcd@2019-09-08 06:04:32+02",
            "TEST@2019-09-08 06:04:32+02",
            "This is a Test@2019-09-08 06:04:32+02"
    })
    void testInstSetType(String value) throws SQLException {
        TTextInst tTextInst = new TTextInst(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT, tTextInst.getTemporalType());
    }

    @Test
    void testInvalidEmptyValue() {
        SQLException thrown = assertThrows(
            SQLException.class,
            () -> {
                TTextInst tTextInst = new TTextInst("");
            }
        );
        assertTrue(thrown.getMessage().contains("Value cannot be empty."));
    }
}
