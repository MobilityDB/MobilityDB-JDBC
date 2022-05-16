package com.mobilitydb.jdbc.temporal;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;

/**
 * Wraps a Temporal data type
 * @param <V> - Base type of the temporal data type eg. Integer, Boolean
 */
public abstract class Temporal<V extends Serializable> implements Serializable {
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

    public abstract List<V> getValues();

    public TemporalType getTemporalType() {
        return temporalType;
    }

    @Override
    public String toString() {
        return buildValue();
    }
}
