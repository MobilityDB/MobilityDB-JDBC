package com.mobilitydb.jdbc.unit.tpoint.tgeog;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tpoint.tgeog.*;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TGeogPointTest {
    @Test
    void testConstructor_tGeogPointInst() throws SQLException {
        String value = "Point(0 0)@2019-09-08 06:04:32+02";
        TGeogPoint tGeogPoint = new TGeogPoint(value);
        assertEquals(TemporalType.TEMPORAL_INSTANT, tGeogPoint.getTemporalType());
        assertTrue(tGeogPoint.getTemporal() instanceof TGeogPointInst);
        TGeogPoint another = new TGeogPoint(tGeogPoint.getTemporal());
        assertEquals(tGeogPoint, another);
    }

    @Test
    void testConstructor_tGeogPointInstSet() throws SQLException {
        String value = "{Point(0 0)@2001-01-01 08:00:00+02, Point(2 2)@2001-01-03 08:00:00+02}";
        TGeogPoint tGeogPoint = new TGeogPoint(value);
        assertEquals(TemporalType.TEMPORAL_INSTANT_SET, tGeogPoint.getTemporalType());
        assertTrue(tGeogPoint.getTemporal() instanceof TGeogPointInstSet);
        TGeogPoint another = new TGeogPoint(tGeogPoint.getTemporal());
        assertEquals(tGeogPoint, another);
    }

    @Test
    void testConstructor_tGeogPointSeq() throws SQLException {
        String value = "[Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02)";
        TGeogPoint tGeogPoint = new TGeogPoint(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE, tGeogPoint.getTemporalType());
        assertTrue(tGeogPoint.getTemporal() instanceof TGeogPointSeq);
        TGeogPoint another = new TGeogPoint(tGeogPoint.getTemporal());
        assertEquals(tGeogPoint, another);
    }

    @Test
    void testConstructor_tGeogPointSeqSet() throws SQLException {
        String value = "{" +
            "[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
            "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02]}";
        TGeogPoint tGeogPoint = new TGeogPoint(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE_SET, tGeogPoint.getTemporalType());
        assertTrue(tGeogPoint.getTemporal() instanceof TGeogPointSeqSet);
        TGeogPoint another = new TGeogPoint(tGeogPoint.getTemporal());
        assertEquals(tGeogPoint, another);
    }
}
