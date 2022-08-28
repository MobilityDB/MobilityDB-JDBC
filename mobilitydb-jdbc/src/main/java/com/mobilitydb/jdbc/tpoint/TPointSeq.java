package com.mobilitydb.jdbc.tpoint;

import com.mobilitydb.jdbc.temporal.TInstant;
import com.mobilitydb.jdbc.temporal.TSequence;
import com.mobilitydb.jdbc.temporal.delegates.GetTemporalInstantFunction;
import com.mobilitydb.jdbc.tpoint.helpers.SRIDParseResponse;
import com.mobilitydb.jdbc.tpoint.helpers.SRIDParser;
import org.postgis.Point;

import java.sql.SQLException;

/**
 * Base abstract class for TGeomPointSeq and TGeogPointSeq
 * Contains logic for handling SRID
 */
public class TPointSeq extends TSequence<Point> {
    private int srid;

    protected TPointSeq(String value, GetTemporalInstantFunction<Point> getTemporalInstantFunction)
            throws SQLException {
        super(value, getTemporalInstantFunction, TPoint::compareValue);
        this.srid = SRIDParser.applySRID(srid, getValues());
    }

    protected TPointSeq(int srid, boolean stepwise, String[] values,
                        GetTemporalInstantFunction<Point> getTemporalInstantFunction) throws SQLException {
        super(stepwise, values, getTemporalInstantFunction, TPoint::compareValue);
        this.srid = SRIDParser.applySRID(srid, getValues());
    }

    protected TPointSeq(int srid, boolean stepwise, String[] values, boolean lowerInclusive, boolean upperInclusive,
                        GetTemporalInstantFunction<Point> getTemporalInstantFunction) throws SQLException {
        super(stepwise, values, lowerInclusive, upperInclusive, getTemporalInstantFunction, TPoint::compareValue);
        this.srid = SRIDParser.applySRID(srid, getValues());
    }

    protected TPointSeq(int srid, boolean stepwise, TInstant<Point>[] values) throws SQLException {
        super(stepwise, values, TPoint::compareValue);
        this.srid = SRIDParser.applySRID(srid, getValues());
    }

    protected TPointSeq(int srid, boolean stepwise, TInstant<Point>[] values,
                        boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(stepwise, values, lowerInclusive, upperInclusive, TPoint::compareValue);
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
