package edu.ulb.mobilitydb.jdbc.temporal;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * Wraps a Temporal data type
 * @param <V> - Base type of the temporal data type eg. Integer, Boolean
 */
public abstract class Temporal<V> implements Serializable {
    protected TemporalType temporalType;

    protected Temporal(TemporalType temporalType) {
        this.temporalType = temporalType;
    }

    protected void validate() throws SQLException {
        validateTemporalDataType();
    }

    /**
     * Throws an SQLException if Temporal data type is not valid
     * @throws SQLException
     */
    protected abstract void validateTemporalDataType() throws SQLException;

    public abstract String buildValue();

    public TemporalType getTemporalType() {
        return temporalType;
    }

    @Override
    public String toString() {
        return buildValue();
    }
}
