package com.mobilitydb.jdbc.unit.tint;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tint.TIntInst;
import com.mobilitydb.jdbc.tint.TIntSeq;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TIntSeqTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "[1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02)";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TIntInst[] instants = new TIntInst[]{
                new TIntInst(1, dateOne),
                new TIntInst(2, dateTwo)
        };
        String[] stringInstants = new String[]{
                "1@2001-01-01 08:00:00+02",
                "2@2001-01-03 08:00:00+02"
        };

        TIntSeq firstTemporal = new TIntSeq(value);
        TIntSeq secondTemporal = new TIntSeq(instants);
        TIntSeq thirdTemporal = new TIntSeq(stringInstants);
        TIntSeq fourthTemporal = new TIntSeq(stringInstants, true, false);
        TIntSeq fifthTemporal = new TIntSeq(stringInstants, true, false);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
        assertEquals(firstTemporal, fourthTemporal);
        assertEquals(firstTemporal, fifthTemporal);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "[1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02)";
        String secondValue = "(1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02]";
        String thirdValue = "[1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)";

        TIntSeq firstTemporal = new TIntSeq(firstValue);
        TIntSeq secondTemporal = new TIntSeq(secondValue);
        TIntSeq thirdTemporal = new TIntSeq(thirdValue);

        assertNotEquals(firstTemporal, secondTemporal);
        assertNotEquals(firstTemporal, thirdTemporal);
        assertNotEquals(secondTemporal, thirdTemporal);
        assertNotEquals(firstTemporal, new Object());
    }

    @Test
    void testIntSeqType() throws SQLException {
        String value = "[1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02)";
        TIntSeq temporal = new TIntSeq(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE, temporal.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        String value = String.format(
                "[1@2001-01-01 08:00:00%1$s, 2@2001-01-03 08:00:00%1$s)",
                tz.toString().substring(0, 3)
        );
        TIntSeq temporal = new TIntSeq(value);
        String newValue = temporal.buildValue();
        assertEquals(value, newValue);
    }
}
