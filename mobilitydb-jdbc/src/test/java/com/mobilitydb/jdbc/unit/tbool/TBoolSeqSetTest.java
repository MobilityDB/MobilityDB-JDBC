package com.mobilitydb.jdbc.unit.tbool;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tbool.TBoolSeq;
import com.mobilitydb.jdbc.tbool.TBoolSeqSet;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TBoolSeqSetTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "{[false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                "[false@2001-01-04 08:00:00+02, true@2001-01-05 08:00:00+02, true@2001-01-06 08:00:00+02]}";

        TBoolSeq[] sequences = new TBoolSeq[]{
            new TBoolSeq("[false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02)"),
            new TBoolSeq("[false@2001-01-04 08:00:00+02, true@2001-01-05 08:00:00+02, true@2001-01-06 08:00:00+02]")
        };
        String[] stringSequences = new String[]{
            "[false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02)",
            "[false@2001-01-04 08:00:00+02, true@2001-01-05 08:00:00+02, true@2001-01-06 08:00:00+02]"
        };

        TBoolSeqSet firstTemporal = new TBoolSeqSet(value);
        TBoolSeqSet secondTemporal = new TBoolSeqSet(sequences);
        TBoolSeqSet thirdTemporal = new TBoolSeqSet(stringSequences);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "{[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02), " +
                "[true@2001-01-04 08:00:00+02, true@2001-01-05 08:00:00+02, true@2001-01-06 08:00:00+02]}";
        String secondValue = "{[false@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02)}";
        String thirdValue = "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}";

        TBoolSeqSet firstTemporal = new TBoolSeqSet(firstValue);
        TBoolSeqSet secondTemporal = new TBoolSeqSet(secondValue);
        TBoolSeqSet thirdTemporal = new TBoolSeqSet(thirdValue);

        assertNotEquals(firstTemporal, secondTemporal);
        assertNotEquals(firstTemporal, thirdTemporal);
        assertNotEquals(secondTemporal, thirdTemporal);
        assertNotEquals(firstTemporal, new Object());
    }

    @Test
    void testBoolSeqSetType() throws SQLException {
        String value = "{[false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02, true@2001-01-06 08:00:00+02]}";
        TBoolSeqSet temporal = new TBoolSeqSet(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE_SET, temporal.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        String value = String.format(
            "{[true@2001-01-01 08:00:00%1$s, true@2001-01-03 08:00:00%1$s), " +
                "[false@2001-01-04 08:00:00%1$s, false@2001-01-05 08:00:00%1$s, true@2001-01-06 08:00:00%1$s]}",
            tz.toString().substring(0, 3)
        );
        TBoolSeqSet temporal = new TBoolSeqSet(value);
        String newValue = temporal.buildValue();
        assertEquals(value, newValue);
    }
}
