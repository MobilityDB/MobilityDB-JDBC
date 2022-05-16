package com.mobilitydb.jdbc.tpoint.tgeom;

import com.mobilitydb.jdbc.temporal.TSequenceSet;
import com.mobilitydb.jdbc.tpoint.TPoint;
import org.postgis.Point;

import java.sql.SQLException;

public class TGeomPointSeqSet extends TSequenceSet<Point> {
    public TGeomPointSeqSet(String value) throws SQLException {
        super(value, TPoint::getSingleTemporalValue);
    }

    public TGeomPointSeqSet(String[] values) throws SQLException {
        super(values, TPoint::getSingleTemporalValue);
    }

    public TGeomPointSeqSet(TGeomPointSeq[] values) throws SQLException {
        super(values, TPoint::getSingleTemporalValue);
    }
}
