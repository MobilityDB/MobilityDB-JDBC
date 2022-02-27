package edu.ulb.mobilitydb.jdbc.temporal;

import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.MobilityDBException;

import java.sql.SQLException;

/**
 * Wraps a Temporal data type
 * @param <V> - Base type of the temporal data type eg. Integer, Boolean
 * @param <T> - Temporal Data Type eg. TInt, TBool
 */
public abstract class Temporal<V, T extends DataType & TemporalDataType<V>>  {
    protected TemporalType type;
    //Temporal data type eg. TInt, TBool
    protected T temporalDataType;

    protected Temporal(TemporalType type) {
        this.type = type;
    }

    protected void validate() throws SQLException {
        validateTemporalType();
        validateTemporalDataType();
    }

    /**
     * Throws an MobilityDBException if Temporal data type is not valid
     * @throws MobilityDBException
     */
    protected abstract void validateTemporalDataType() throws SQLException;

    /**
     * Verifies that the given temporal data type has the correct temporal type
     * @throws MobilityDBException
     */
    protected void validateTemporalType() throws SQLException {
        if (temporalDataType.getTemporalType() != type) {
            throw new MobilityDBException("Invalid temporal type.");
        }
    }

    protected abstract String buildValue();

    /**
     * Allow the non abstract classes to convert the object used in equals method
     * to the correct type
     * @param obj
     * @return
     */
    protected abstract Temporal<V,T> convert(Object obj);

    public T getDataType() {
        return temporalDataType;
    }

    @Override
    public String toString() {
        return buildValue();
    }
}
