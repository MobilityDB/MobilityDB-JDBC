package edu.ulb.mobilitydb.jdbc.tpoint.tgeom;

import edu.ulb.mobilitydb.jdbc.temporal.TSequence;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;
import org.postgis.Point;

import java.sql.SQLException;

public class TGeomPointSeq extends TSequence<Point, TGeomPoint> {

    public TGeomPointSeq(TGeomPoint temporalDataType) throws SQLException {
        super(temporalDataType);
    }

    public TGeomPointSeq(String value) throws SQLException {
        super(TGeomPoint::new, value);
    }

    public TGeomPointSeq(String[] values) throws SQLException {
        super(TGeomPoint::new, values);
    }

    public TGeomPointSeq(String[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(TGeomPoint::new, values, lowerInclusive, upperInclusive);
    }

    public TGeomPointSeq(TGeomPointInst[] values) throws SQLException {
        super(TGeomPoint::new, values);
    }

    public TGeomPointSeq(TGeomPointInst[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(TGeomPoint::new, values, lowerInclusive, upperInclusive);
    }

    @Override
    protected Temporal<Point, TGeomPoint> convert(Object obj) {
        if (obj instanceof TGeomPointSeq) {
            return (TGeomPointSeq) obj;
        }
        return null;
    }
}
