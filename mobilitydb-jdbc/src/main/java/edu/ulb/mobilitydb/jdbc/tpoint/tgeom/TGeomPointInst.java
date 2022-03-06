package edu.ulb.mobilitydb.jdbc.tpoint.tgeom;

import edu.ulb.mobilitydb.jdbc.temporal.TInstant;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;
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

    @Override
    protected Temporal<Point> convert(Object obj) {
        if (obj instanceof TGeomPointInst) {
            return (TGeomPointInst) obj;
        }
        return null;
    }
}
