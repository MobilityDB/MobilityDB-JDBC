package edu.ulb.mobilitydb.jdbc.tpoint.tgeom;

import edu.ulb.mobilitydb.jdbc.temporal.TSequence;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;
import org.postgis.Point;

import java.sql.SQLException;

public class TGeomPointSeq extends TSequence<Point> {
    public TGeomPointSeq(String value) throws SQLException {
        super(value, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeq(String[] values) throws SQLException {
        super(values, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeq(String[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(values, lowerInclusive, upperInclusive, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeq(TGeomPointInst[] values) throws SQLException {
        super(values);
    }

    public TGeomPointSeq(TGeomPointInst[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(values, lowerInclusive, upperInclusive);
    }

    @Override
    protected Temporal<Point> convert(Object obj) {
        if (obj instanceof TGeomPointSeq) {
            return (TGeomPointSeq) obj;
        }
        return null;
    }
}
