package edu.ulb.mobilitydb.jdbc.tpoint.tgeog;

import edu.ulb.mobilitydb.jdbc.temporal.TSequence;
import edu.ulb.mobilitydb.jdbc.tpoint.TPoint;
import org.postgis.Point;

import java.sql.SQLException;

public class TGeogPointSeq extends TSequence<Point> {
    public TGeogPointSeq(String value) throws SQLException {
        super(value, TGeogPoint::getSingleTemporalValue);
    }

    public TGeogPointSeq(String[] values) throws SQLException {
        super(values, TGeogPoint::getSingleTemporalValue);
    }

    public TGeogPointSeq(String[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(values, lowerInclusive, upperInclusive, TPoint::getSingleTemporalValue);
    }

    public TGeogPointSeq(TGeogPointInst[] values) throws SQLException {
        super(values);
    }

    public TGeogPointSeq(TGeogPointInst[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(values, lowerInclusive, upperInclusive);
    }
}
