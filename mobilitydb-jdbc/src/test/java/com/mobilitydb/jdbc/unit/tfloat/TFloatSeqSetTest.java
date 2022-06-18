package com.mobilitydb.jdbc.unit.tfloat;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tfloat.TFloatSeq;
import com.mobilitydb.jdbc.tfloat.TFloatSeqSet;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TFloatSeqSetTest {
    @Test
    void testLinearConstructors() throws SQLException {
        String value = "{[1.8@2001-01-01 08:00:00+02, 1.9@2001-01-03 08:00:00+02), " +
                "[2.3@2001-01-04 08:00:00+02, 3.4@2001-01-05 08:00:00+02, 3.5@2001-01-06 08:00:00+02]}";

        TFloatSeq[] sequences = new TFloatSeq[]{
                new TFloatSeq("[1.8@2001-01-01 08:00:00+02, 1.9@2001-01-03 08:00:00+02)"),
                new TFloatSeq("[2.3@2001-01-04 08:00:00+02, 3.4@2001-01-05 08:00:00+02, 3.5@2001-01-06 08:00:00+02]")
        };
        String[] stringSequences = new String[]{
                "[1.8@2001-01-01 08:00:00+02, 1.9@2001-01-03 08:00:00+02)",
                "[2.3@2001-01-04 08:00:00+02, 3.4@2001-01-05 08:00:00+02, 3.5@2001-01-06 08:00:00+02]"
        };

        TFloatSeqSet firstTemporal = new TFloatSeqSet(value);
        TFloatSeqSet secondTemporal = new TFloatSeqSet(sequences);
        TFloatSeqSet thirdTemporal = new TFloatSeqSet(stringSequences);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
    }

    @Test
    void testStepwiseConstructors() throws SQLException {
        String value = "Interp=Stepwise;{[1.8@2001-01-01 08:00:00+02, 1.9@2001-01-03 08:00:00+02), " +
                "[2.3@2001-01-04 08:00:00+02, 3.4@2001-01-05 08:00:00+02, 3.5@2001-01-06 08:00:00+02]}";

        TFloatSeq[] sequences = new TFloatSeq[]{
                new TFloatSeq("Interp=Stepwise;[1.8@2001-01-01 08:00:00+02, 1.9@2001-01-03 08:00:00+02)"),
                new TFloatSeq("Interp=Stepwise;[2.3@2001-01-04 08:00:00+02, 3.4@2001-01-05 08:00:00+02, " +
                        "3.5@2001-01-06 08:00:00+02]")
        };
        String[] stringSequences = new String[]{
                "[1.8@2001-01-01 08:00:00+02, 1.9@2001-01-03 08:00:00+02)",
                "[2.3@2001-01-04 08:00:00+02, 3.4@2001-01-05 08:00:00+02, 3.5@2001-01-06 08:00:00+02]"
        };

        TFloatSeqSet firstTemporal = new TFloatSeqSet(value);
        TFloatSeqSet secondTemporal = new TFloatSeqSet(true, sequences);
        TFloatSeqSet thirdTemporal = new TFloatSeqSet(true,stringSequences);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "{[1.2@2001-01-01 08:00:00+02, 1.3@2001-01-03 08:00:00+02), " +
                "[2.1@2001-01-04 08:00:00+02, 3.8@2001-01-05 08:00:00+02, 3.9@2001-01-06 08:00:00+02]}";
        String secondValue = "{[1.2@2001-01-01 08:00:00+02, 1.3@2001-01-03 08:00:00+02)}";
        String thirdValue = "{[1.2@2001-01-01 08:00:00+02, 1.3@2001-01-03 08:00:00+02), " +
                "[2.1@2001-01-04 08:00:00+02, 3.8@2001-01-05 08:00:00+02]}";

        TFloatSeqSet firstTemporal = new TFloatSeqSet(firstValue);
        TFloatSeqSet secondTemporal = new TFloatSeqSet(secondValue);
        TFloatSeqSet thirdTemporal = new TFloatSeqSet(thirdValue);

        assertNotEquals(firstTemporal, secondTemporal);
        assertNotEquals(firstTemporal, thirdTemporal);
        assertNotEquals(secondTemporal, thirdTemporal);
        assertNotEquals(firstTemporal, new Object());
    }

    @Test
    void testSeqSetType() throws SQLException {
        String value = "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}";
        TFloatSeqSet temporal = new TFloatSeqSet(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE_SET, temporal.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        String value = String.format(
                "{[1.2@2001-01-01 08:00:00%1$s, 1.5@2001-01-03 08:00:00%1$s), " +
                        "[2.8@2001-01-04 08:00:00%1$s, 3.2@2001-01-05 08:00:00%1$s, 3.3@2001-01-06 08:00:00%1$s]}",
                tz.toString().substring(0, 3)
        );
        TFloatSeqSet temporal = new TFloatSeqSet(value);
        String newValue = temporal.buildValue();
        assertEquals(value, newValue);
    }
}
