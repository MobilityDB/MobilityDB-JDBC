package edu.ulb.mobilitydb.jdbc.temporal;

import edu.ulb.mobilitydb.jdbc.core.DataType;

public abstract class Temporal<V, T extends DataType & TemporalDataType<V>>  {
    protected TemporalType type;
    protected T temporalDataType; //Tint, Tbool

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

    protected abstract boolean areEqual(Temporal<V, T> otherTemporal);

    protected abstract Temporal<V,T> convert(Object obj);

    public T getDataType() {
        return temporalDataType;
    }

    @Override
    public String toString() {
        return buildValue();
    }

    @Override
    public boolean equals(Object obj) {
        if (getClass() == obj.getClass()) {
            return areEqual(convert(obj));
        }
        return false;
    }
}
