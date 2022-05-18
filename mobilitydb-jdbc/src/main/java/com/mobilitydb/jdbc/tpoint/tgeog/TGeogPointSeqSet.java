package com.mobilitydb.jdbc.tpoint.tgeog;

import com.mobilitydb.jdbc.temporal.TSequenceSet;
import com.mobilitydb.jdbc.tpoint.TPoint;
import org.postgis.Point;

import java.sql.SQLException;

public class TGeogPointSeqSet extends TSequenceSet<Point> {
    public TGeogPointSeqSet(String value) throws SQLException {
        super(value, TPoint::getSingleTemporalValue);
    }

    public TGeogPointSeqSet(boolean stepwise, String[] values) throws SQLException {
        super(stepwise, values, TPoint::getSingleTemporalValue);
    }

    public TGeogPointSeqSet(boolean stepwise, TGeogPointSeq[] values) throws SQLException {
        super(stepwise, values, TPoint::getSingleTemporalValue);
    }
}
