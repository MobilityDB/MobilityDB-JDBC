package edu.ulb.mobilitydb.jdbc.temporal;

import java.sql.SQLException;

/**
 * Interface for Temporal Data Types (eg TInt, TFloat) common methods
 * @param <T>
 */
public interface TemporalDataType<T> {
    TemporalType getTemporalType();
    TemporalValue<T> getSingleTemporalValue(String value) throws SQLException;
}
