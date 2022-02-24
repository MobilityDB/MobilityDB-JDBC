package edu.ulb.mobilitydb.jdbc.temporal;

/**
 * Interface for Temporal Data Types (eg TInt, TFloat) common methods
 * @param <T>
 */
public interface TemporalDataType<T> {
    TemporalType getTemporalType();
    TemporalValue<T> getSingleTemporalValue(String value);
}
