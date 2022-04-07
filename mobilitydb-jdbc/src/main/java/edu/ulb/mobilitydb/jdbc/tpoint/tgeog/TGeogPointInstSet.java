package edu.ulb.mobilitydb.jdbc.tpoint.tgeog;

import edu.ulb.mobilitydb.jdbc.temporal.TInstantSet;
import org.postgis.Point;

import java.sql.SQLException;

public class TGeogPointInstSet extends TInstantSet<Point> {
    public TGeogPointInstSet(String value) throws SQLException {
        super(value, TGeogPoint::getSingleTemporalValue);
    }

    public TGeogPointInstSet(String[] values) throws SQLException {
        super(values, TGeogPoint::getSingleTemporalValue);
    }

    public TGeogPointInstSet(TGeogPointInst[] values) throws SQLException {
        super(values);
    }
}
