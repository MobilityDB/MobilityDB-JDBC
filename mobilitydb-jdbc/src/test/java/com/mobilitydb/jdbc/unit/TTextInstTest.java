package com.mobilitydb.jdbc.unit;

import com.mobilitydb.jdbc.tint.TIntInst;
import com.mobilitydb.jdbc.ttext.TTextInst;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
