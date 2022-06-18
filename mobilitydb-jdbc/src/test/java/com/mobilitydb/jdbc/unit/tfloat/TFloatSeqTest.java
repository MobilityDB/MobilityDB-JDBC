package com.mobilitydb.jdbc.unit.tfloat;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tfloat.TFloatInst;
import com.mobilitydb.jdbc.tfloat.TFloatSeq;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TFloatSeqTest {
    @Test
    void testLinearConstructors() throws SQLException {
        String value = "[1.85@2001-01-01 08:00:00+02, 2.85@2001-01-03 08:00:00+02)";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TFloatInst[] instants = new TFloatInst[]{
                new TFloatInst(1.85f, dateOne),
                new TFloatInst(2.85f, dateTwo)
        };
        String[] stringInstants = new String[]{
                "1.85@2001-01-01 08:00:00+02",
                "2.85@2001-01-03 08:00:00+02"
        };

        TFloatSeq firstTemporal = new TFloatSeq(value);
        TFloatSeq secondTemporal = new TFloatSeq(instants);
        TFloatSeq thirdTemporal = new TFloatSeq(stringInstants);
        TFloatSeq fourthTemporal = new TFloatSeq(instants, true, false);
        TFloatSeq fifthTemporal = new TFloatSeq(stringInstants, true, false);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
        assertEquals(firstTemporal, fourthTemporal);
        assertEquals(firstTemporal, fifthTemporal);
    }

    @Test
    void testStepwiseConstructors() throws SQLException {
        String value = "Interp=Stepwise;[2.5@2001-01-01 08:00:00+02, 3.6@2001-01-03 08:00:00+02)";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TFloatInst[] instants = new TFloatInst[]{
                new TFloatInst(2.5f, dateOne),
                new TFloatInst(3.6f, dateTwo)
        };
        String[] stringInstants = new String[]{
                "2.5@2001-01-01 08:00:00+02",
                "3.6@2001-01-03 08:00:00+02"
        };

        TFloatSeq firstTemporal = new TFloatSeq(value);
        TFloatSeq secondTemporal = new TFloatSeq(true,instants);
        TFloatSeq thirdTemporal = new TFloatSeq(true,stringInstants);
        TFloatSeq fourthTemporal = new TFloatSeq(true,instants, true, false);
        TFloatSeq fifthTemporal = new TFloatSeq(true, stringInstants, true, false);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
        assertEquals(firstTemporal, fourthTemporal);
        assertEquals(firstTemporal, fifthTemporal);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "[1.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02)";
        String secondValue = "(1.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02]";
        String thirdValue = "[1.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)";
        String fourthValue = "Interp=Stepwise;(1.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02]";

        TFloatSeq firstTemporal = new TFloatSeq(firstValue);
        TFloatSeq secondTemporal = new TFloatSeq(secondValue);
        TFloatSeq thirdTemporal = new TFloatSeq(thirdValue);
        TFloatSeq fourthTemporal = new TFloatSeq(fourthValue);

        assertNotEquals(firstTemporal, secondTemporal);
        assertNotEquals(firstTemporal, thirdTemporal);
        assertNotEquals(secondTemporal, thirdTemporal);
        assertNotEquals(secondTemporal, fourthTemporal);
        assertNotEquals(firstTemporal, new Object());
    }

    @Test
    void testSeqType() throws SQLException {
        String value = "[87.2@2001-01-01 08:00:00+02, 92.1@2001-01-03 08:00:00+02)";
        TFloatSeq temporal = new TFloatSeq(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE, temporal.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        String value = String.format(
                "[7.45@2001-01-01 08:00:00%1$s, 9.81@2001-01-03 08:00:00%1$s)",
                tz.toString().substring(0, 3)
        );
        TFloatSeq temporal = new TFloatSeq(value);
        String newValue = temporal.buildValue();
        assertEquals(value, newValue);
    }
}
