package com.mobilitydb.jdbc.tpoint.tgeom;

import com.mobilitydb.jdbc.temporal.TSequenceSet;
import com.mobilitydb.jdbc.tpoint.TPoint;
import org.postgis.Point;

import java.sql.SQLException;

public class TGeomPointSeqSet extends TSequenceSet<Point> {
    public TGeomPointSeqSet(String value) throws SQLException {
        super(value, TPoint::getSingleTemporalValue);
    }

    public TGeomPointSeqSet(boolean stepwise, String[] values) throws SQLException {
        super(stepwise, values, TPoint::getSingleTemporalValue);
    }

    public TGeomPointSeqSet(boolean stepwise, TGeomPointSeq[] values) throws SQLException {
        super(stepwise, values, TPoint::getSingleTemporalValue);
    }
}
