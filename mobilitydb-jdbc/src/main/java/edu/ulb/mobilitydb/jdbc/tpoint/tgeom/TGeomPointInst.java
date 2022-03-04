package edu.ulb.mobilitydb.jdbc.tpoint.tgeom;

import edu.ulb.mobilitydb.jdbc.temporal.TInstant;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;
import org.postgis.Point;

import java.sql.SQLException;
import java.time.OffsetDateTime;

public class TGeomPointInst extends TInstant<Point, TGeomPoint> {

    public TGeomPointInst(TGeomPoint temporalDataType) throws SQLException {
        super(temporalDataType);
    }

    public TGeomPointInst(String value) throws SQLException {
        super(TGeomPoint::new, value);
    }

    public TGeomPointInst(Point value, OffsetDateTime time) throws SQLException {
        super(TGeomPoint::new, value, time);
    }

    @Override
    protected Temporal<Point, TGeomPoint> convert(Object obj) {
        if (obj instanceof TGeomPointInst) {
            return (TGeomPointInst) obj;
        }
        return null;
    }
}
