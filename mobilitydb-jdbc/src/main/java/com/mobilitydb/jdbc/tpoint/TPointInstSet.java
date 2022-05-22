package com.mobilitydb.jdbc.tpoint;

import com.mobilitydb.jdbc.temporal.GetSingleTemporalValueFunction;
import com.mobilitydb.jdbc.temporal.TInstant;
import com.mobilitydb.jdbc.temporal.TInstantSet;
import com.mobilitydb.jdbc.temporal.TemporalValue;
import org.postgis.Point;

import java.sql.SQLException;

public abstract class TPointInstSet extends TInstantSet<Point> {
    protected int srid;

    protected TPointInstSet(String value, GetSingleTemporalValueFunction<Point> getSingleTemporalValue)
            throws SQLException {
        super(value, getSingleTemporalValue);
        processSRID();
    }

    protected TPointInstSet(int srid, String[] values, GetSingleTemporalValueFunction<Point> getSingleTemporalValue)
            throws SQLException {
        super(values, getSingleTemporalValue);
        this.srid = srid;
        processSRID();
    }

    protected TPointInstSet(int srid, TInstant<Point>[] values) throws SQLException {
        super(values);
        this.srid = srid;
        processSRID();
    }

    @Override
    protected String preprocessValue(String value) throws SQLException {
        String newString = super.preprocessValue(value);

        if (newString.startsWith(TPointConstants.SRID_PREFIX)) {
            int idx = newString.indexOf(";");
            String sridString = newString.substring(TPointConstants.SRID_PREFIX.length(), idx);
            newString = newString.substring(idx + 1);
            srid = Integer.parseInt(sridString);

            if (srid < 0) {
                throw new SQLException("Invalid SRID");
            }
        }

        return newString;
    }

    private void processSRID() throws SQLException {
        if (srid == TPointConstants.EMPTY_SRID) {
            srid = getFirstSRID();

            if (srid == TPointConstants.EMPTY_SRID) {
                return;
            }
        }

        for (TemporalValue<Point> temporalValue : temporalValues) {
            int currentSRID = temporalValue.getValue().getSrid();

            if (currentSRID == TPointConstants.EMPTY_SRID) {
                currentSRID = srid;
                temporalValue.getValue().setSrid(currentSRID);
            }

            if (currentSRID != srid) {
                throw new SQLException(String.format(
                        "Geometry SRID (%d) does not match temporal type SRID (%d)", currentSRID, srid
                ));
            }
        }
    }

    private int getFirstSRID() {
        for (TemporalValue<Point> temporalValue : temporalValues) {
            int currentSRID = temporalValue.getValue().getSrid();

            if (currentSRID != TPointConstants.EMPTY_SRID) {
                return currentSRID;
            }
        }

        return TPointConstants.EMPTY_SRID;
    }

    @Override
    public boolean equals(Object obj) {
        // It is not required to verify if the SRID are equals since it is applied to the temporal values
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        // It is not required to include the SRID since it is applied to the temporal values
        return super.hashCode();
    }
}
