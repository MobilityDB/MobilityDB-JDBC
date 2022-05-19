package com.mobilitydb.jdbc.tpoint.tgeom;

import com.mobilitydb.jdbc.temporal.TSequenceSet;
import org.postgis.Point;

import java.sql.SQLException;

public class TGeomPointSeqSet extends TSequenceSet<Point> {
    public TGeomPointSeqSet(String value) throws SQLException {
        super(value, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeqSet(boolean stepwise, String[] values) throws SQLException {
        super(stepwise, values, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeqSet(boolean stepwise, TGeomPointSeq[] values) throws SQLException {
        super(stepwise, values, TGeomPoint::getSingleTemporalValue);
    }
}
