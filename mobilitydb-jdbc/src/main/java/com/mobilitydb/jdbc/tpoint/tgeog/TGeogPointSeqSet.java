package com.mobilitydb.jdbc.tpoint.tgeog;

import com.mobilitydb.jdbc.temporal.TSequenceSet;
import org.postgis.Point;

import java.sql.SQLException;

public class TGeogPointSeqSet extends TSequenceSet<Point> {
    public TGeogPointSeqSet(String value) throws SQLException {
        super(value, TGeogPoint::getSingleTemporalValue);
    }

    public TGeogPointSeqSet(boolean stepwise, String[] values) throws SQLException {
        super(stepwise, values, TGeogPoint::getSingleTemporalValue);
    }

    public TGeogPointSeqSet(boolean stepwise, TGeogPointSeq[] values) throws SQLException {
        super(stepwise, values, TGeogPoint::getSingleTemporalValue);
    }
}
