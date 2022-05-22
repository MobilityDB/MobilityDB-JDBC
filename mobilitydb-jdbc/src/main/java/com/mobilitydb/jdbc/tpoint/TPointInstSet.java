package com.mobilitydb.jdbc.tpoint;

import com.mobilitydb.jdbc.temporal.GetSingleTemporalValueFunction;
import com.mobilitydb.jdbc.temporal.TInstant;
import com.mobilitydb.jdbc.temporal.TInstantSet;
import org.postgis.Point;

import java.sql.SQLException;

public abstract class TPointInstSet extends TInstantSet<Point> {
    protected int srid;

    protected TPointInstSet(String value, GetSingleTemporalValueFunction<Point> getSingleTemporalValue)
            throws SQLException {
        super(value, getSingleTemporalValue);
    }

    protected TPointInstSet(int srid, String[] values, GetSingleTemporalValueFunction<Point> getSingleTemporalValue)
            throws SQLException {
        super(values, getSingleTemporalValue);
        this.srid = srid;
    }

    protected TPointInstSet(int srid, TInstant<Point>[] values) throws SQLException {
        super(values);
        this.srid = srid;
    }

    @Override
    protected String preprocessValue(String value) throws SQLException {
        String newString = super.preprocessValue(value);

        if (newString.startsWith("SRID=")) {
            int idx = newString.indexOf(";");
            String sridString = newString.substring(5, idx);
            newString = newString.substring(idx + 1);
            srid = Integer.parseInt(sridString);

            if (srid < 0) {
                throw new SQLException("Invalid SRID");
            }
        }

        return newString;
    }
}
