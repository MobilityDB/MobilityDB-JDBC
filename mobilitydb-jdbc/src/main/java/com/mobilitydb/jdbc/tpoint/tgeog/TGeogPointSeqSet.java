package com.mobilitydb.jdbc.tpoint.tgeog;

import com.mobilitydb.jdbc.temporal.TSequenceSet;
import com.mobilitydb.jdbc.tpoint.TPoint;
import org.postgis.Point;

import java.sql.SQLException;

public class TGeogPointSeqSet extends TSequenceSet<Point> {
    public TGeogPointSeqSet(String value) throws SQLException {
        super(value, TPoint::getSingleTemporalValue);
    }

    public TGeogPointSeqSet(String[] values) throws SQLException {
        super(values, TPoint::getSingleTemporalValue);
    }

    public TGeogPointSeqSet(TGeogPointSeq[] values) throws SQLException {
        super(values, TPoint::getSingleTemporalValue);
    }
}
