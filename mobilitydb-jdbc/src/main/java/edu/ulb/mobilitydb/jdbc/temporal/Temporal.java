package edu.ulb.mobilitydb.jdbc.temporal;

import java.sql.SQLException;

/**
 * Wraps a Temporal data type
 * @param <V> - Base type of the temporal data type eg. Integer, Boolean
 */
public abstract class Temporal<V>  {
    protected TemporalType temporalType;

    protected Temporal(TemporalType temporalType) {
        this.temporalType = temporalType;
    }

    protected void validate() throws SQLException {
        validateTemporalDataType();
    }

    /**
     * Throws an MobilityDBException if Temporal data type is not valid
     * @throws SQLException
     */
    protected abstract void validateTemporalDataType() throws SQLException;

    public abstract String buildValue();

    /**
     * Allow the non abstract classes to convert the object used in equals method
     * to the correct type
     * @param obj
     * @return
     */
    protected abstract Temporal<V> convert(Object obj);

    public TemporalType getTemporalType() {
        return temporalType;
    }

    @Override
    public String toString() {
        return buildValue();
    }
}
