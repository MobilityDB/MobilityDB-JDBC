package edu.ulb.mobilitydb.jdbc.temporal;

import edu.ulb.mobilitydb.jdbc.core.DataType;

/**
 * Wraps a Temporal data type
 * @param <V> - Base type of the temporal data type eg. Integer, Boolean
 * @param <T> - Temporal Data Type eg. TInt, TBool
 */
public abstract class Temporal<V, T extends DataType & TemporalDataType<V>>  {
    protected TemporalType type;
    // Eg. TInt, TBool
    protected T temporalDataType;

    public Temporal(TemporalType type) {
        this.type = type;
    }

    protected void validate() throws Exception {
        validateTemporalType();
        validateTemporalDataType();
    }

    /**
     * Throws an exception if Temporal data type is not valid
     * @throws Exception
     */
    protected abstract void validateTemporalDataType() throws Exception;

    /**
     * Verifies that the given temporal data type has the correct temporal type
     * @throws Exception
     */
    protected void validateTemporalType() throws Exception {
        if (temporalDataType.getTemporalType() != type) {
            throw new Exception("Invalid temporal type.");
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
