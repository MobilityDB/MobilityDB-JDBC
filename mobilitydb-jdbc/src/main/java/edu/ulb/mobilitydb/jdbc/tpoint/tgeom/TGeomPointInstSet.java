package edu.ulb.mobilitydb.jdbc.tpoint.tgeom;

import edu.ulb.mobilitydb.jdbc.temporal.TInstantSet;
import org.postgis.Point;

import java.sql.SQLException;

public class TGeomPointInstSet extends TInstantSet<Point> {
    public TGeomPointInstSet(String value) throws SQLException {
        super(value, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointInstSet(String[] values) throws SQLException {
        super(values, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointInstSet(TGeomPointInst[] values) throws SQLException {
        super(values);
    }
}
