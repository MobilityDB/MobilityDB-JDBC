package com.mobilitydb.jdbc.unit.tpoint.tgeom;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointSeq;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointSeqSet;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TGeomPointSeqSetTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "{" +
            "[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
            "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}";

        TGeomPointSeq[] sequences = new TGeomPointSeq[]{
            new TGeomPointSeq("[" +
                    "010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)"),
            new TGeomPointSeq("[" +
                    "010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]")
        };
        String[] stringSequences = new String[]{
            "[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)",
            "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]"
        };

        TGeomPointSeqSet firstTemporal = new TGeomPointSeqSet(value);
        TGeomPointSeqSet secondTemporal = new TGeomPointSeqSet(sequences);
        TGeomPointSeqSet thirdTemporal = new TGeomPointSeqSet(stringSequences);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}";
        String secondValue = "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)}";
        String thirdValue = "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02]}";

        TGeomPointSeqSet firstTemporal = new TGeomPointSeqSet(firstValue);
        TGeomPointSeqSet secondTemporal = new TGeomPointSeqSet(secondValue);
        TGeomPointSeqSet thirdTemporal = new TGeomPointSeqSet(thirdValue);

        assertNotEquals(firstTemporal, secondTemporal);
        assertNotEquals(firstTemporal, thirdTemporal);
        assertNotEquals(secondTemporal, thirdTemporal);
        assertNotEquals(firstTemporal, new Object());
    }

    @Test
    void testSeqSetType() throws SQLException {
        String value = "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}";
        TGeomPointSeqSet temporal = new TGeomPointSeqSet(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE_SET, temporal.getTemporalType());
    }
}
