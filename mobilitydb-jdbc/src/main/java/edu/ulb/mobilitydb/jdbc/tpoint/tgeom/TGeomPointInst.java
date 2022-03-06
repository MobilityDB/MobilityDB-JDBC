package edu.ulb.mobilitydb.jdbc.tpoint.tgeom;

import edu.ulb.mobilitydb.jdbc.temporal.TInstant;
import org.postgis.Point;

import java.sql.SQLException;
import java.time.OffsetDateTime;

public class TGeomPointInst extends TInstant<Point> {
    public TGeomPointInst(String value) throws SQLException {
        super(value, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointInst(Point value, OffsetDateTime time) throws SQLException {
        super(value, time);
    }
}
