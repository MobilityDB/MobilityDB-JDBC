package edu.ulb.mobilitydb.jdbc.tpoint.tgeog;

import edu.ulb.mobilitydb.jdbc.temporal.TInstant;
import org.postgis.Point;

import java.sql.SQLException;
import java.time.OffsetDateTime;

public class TGeogPointInst extends TInstant<Point> {
    public TGeogPointInst(String value) throws SQLException {
        super(value, TGeogPoint::getSingleTemporalValue);
    }

    public TGeogPointInst(Point value, OffsetDateTime time) throws SQLException {
        super(value, time);
    }
}
