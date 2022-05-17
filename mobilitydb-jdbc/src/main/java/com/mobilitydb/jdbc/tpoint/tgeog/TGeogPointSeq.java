package com.mobilitydb.jdbc.tpoint.tgeog;

import com.mobilitydb.jdbc.temporal.TSequence;
import com.mobilitydb.jdbc.tpoint.TPoint;
import org.postgis.Point;

import java.sql.SQLException;

public class TGeogPointSeq extends TSequence<Point> {
    public TGeogPointSeq(String value) throws SQLException {
        super(value, TGeogPoint::getSingleTemporalValue);
    }

    public TGeogPointSeq(boolean isStepwise, String[] values) throws SQLException {
        super(isStepwise, values, TGeogPoint::getSingleTemporalValue);
    }

    public TGeogPointSeq(boolean isStepwise, String[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(isStepwise, values, lowerInclusive, upperInclusive, TPoint::getSingleTemporalValue);
    }

    public TGeogPointSeq(boolean isStepwise, TGeogPointInst[] values) throws SQLException {
        super(isStepwise, values);
    }

    public TGeogPointSeq(boolean isStepwise, TGeogPointInst[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(isStepwise, values, lowerInclusive, upperInclusive);
    }
}
