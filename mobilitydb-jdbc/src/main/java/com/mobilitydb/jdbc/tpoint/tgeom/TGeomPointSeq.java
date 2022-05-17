package com.mobilitydb.jdbc.tpoint.tgeom;

import com.mobilitydb.jdbc.temporal.TSequence;
import org.postgis.Point;

import java.sql.SQLException;

public class TGeomPointSeq extends TSequence<Point> {
    public TGeomPointSeq(String value) throws SQLException {
        super(value, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeq(boolean isStepwise, String[] values) throws SQLException {
        super(isStepwise, values, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeq(boolean isStepwise, String[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(isStepwise, values, lowerInclusive, upperInclusive, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeq(boolean isStepwise, TGeomPointInst[] values) throws SQLException {
        super(isStepwise, values);
    }

    public TGeomPointSeq(boolean isStepwise, TGeomPointInst[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(isStepwise, values, lowerInclusive, upperInclusive);
    }
}
