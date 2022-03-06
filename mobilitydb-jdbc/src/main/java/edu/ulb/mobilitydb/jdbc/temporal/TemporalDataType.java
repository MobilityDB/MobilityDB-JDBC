package edu.ulb.mobilitydb.jdbc.temporal;

import java.sql.SQLException;

/**
 * Interface for Temporal Data Types (eg TInt, TFloat) common methods
 * @param <V>
 */
public interface TemporalDataType<V> {
    Temporal<V> getTemporal();
    TemporalType getTemporalType();
}
