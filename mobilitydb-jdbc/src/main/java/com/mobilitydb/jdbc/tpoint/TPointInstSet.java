package com.mobilitydb.jdbc.tpoint;

import com.mobilitydb.jdbc.temporal.TInstant;
import com.mobilitydb.jdbc.temporal.TInstantSet;
import com.mobilitydb.jdbc.temporal.delegates.GetTemporalInstantFunction;
import com.mobilitydb.jdbc.tpoint.helpers.SRIDParseResponse;
import com.mobilitydb.jdbc.tpoint.helpers.SRIDParser;
import org.postgis.Point;

import java.sql.SQLException;

/**
 * Base abstract class for TGeomPointIntSet and TGeogPointIntSet
 * Contains logic for handling SRID
 */
public abstract class TPointInstSet extends TInstantSet<Point> {
    private int srid;

    protected TPointInstSet(String value, GetTemporalInstantFunction<Point> getTemporalInstantFunction)
            throws SQLException {
        super(value, getTemporalInstantFunction, TPoint::compareValue);
        this.srid = SRIDParser.applySRID(srid, getValues());
    }

    protected TPointInstSet(int srid, String[] values, GetTemporalInstantFunction<Point> getTemporalInstantFunction)
            throws SQLException {
        super(values, getTemporalInstantFunction, TPoint::compareValue);
        this.srid = SRIDParser.applySRID(srid, getValues());
    }

    protected TPointInstSet(int srid, TInstant<Point>[] values) throws SQLException {
        super(values, TPoint::compareValue);
        this.srid = SRIDParser.applySRID(srid, getValues());
    }

    /**
     * Parse the SRID value
     * @param value - a string with the value
     * @return the string without SRID
     * @throws SQLException if it is invalid
     */
    @Override
    protected String preprocessValue(String value) throws SQLException {
        String newString = super.preprocessValue(value);
        SRIDParseResponse response = SRIDParser.parseSRID(newString);
        srid = response.getSRID();
        return response.getParsedValue();
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
