package com.mobilitydb.jdbc.temporal;

import com.mobilitydb.jdbc.core.DataType;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * Interface for Temporal Data Types (eg TInt, TFloat) common methods
 * @param <V>
 */
public abstract class TemporalDataType<V extends Serializable> extends DataType {
    protected Temporal<V> temporal;

    protected TemporalDataType() {
        super();
    }

    protected TemporalDataType(final String value) throws SQLException {
        super();
        setValue(value);
    }

    public Temporal<V> getTemporal() {
        return temporal;
    }

    public TemporalType getTemporalType() {
        return temporal.getTemporalType();
    }

    @Override
    public String getValue() {
        return temporal.buildValue();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj instanceof TemporalDataType<?>) {
            TemporalDataType<?> other = (TemporalDataType<?>)obj;
            return temporal.equals(other.temporal);
        }

        return false;
    }

    @Override
    public int hashCode() {
        String value = toString();
        return value != null ? value.hashCode() : 0;
    }
}
