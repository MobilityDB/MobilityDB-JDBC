package edu.ulb.mobilitydb.jdbc.tpoint.tgeom;

import edu.ulb.mobilitydb.jdbc.temporal.TInstantSet;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;
import org.postgis.Point;

import java.sql.SQLException;

public class TGeomPointInstSet extends TInstantSet<Point, TGeomPoint> {

    public TGeomPointInstSet(TGeomPoint temporalDataType) throws SQLException {
        super(temporalDataType);
    }

    public TGeomPointInstSet(String value) throws SQLException {
        super(TGeomPoint::new, value);
    }

    public TGeomPointInstSet(String[] values) throws SQLException {
        super(TGeomPoint::new, values);
    }

    public TGeomPointInstSet(TGeomPointInst[] values) throws SQLException {
        super(TGeomPoint::new, values);
    }

    @Override
    protected Temporal<Point, TGeomPoint> convert(Object obj) {
        if (obj instanceof TGeomPointInstSet) {
            return (TGeomPointInstSet) obj;
        }
        return null;
    }
}
