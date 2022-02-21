package edu.ulb.mobilitydb.jdbc.temporal;

import edu.ulb.mobilitydb.jdbc.core.DataType;

public class TemporalInstant<V, T extends DataType & TemporalDataType<V>> {
    private T temporal; //tint, tbool
    private TemporalValue<V> temporalValue; //int, bool

    public TemporalInstant(T temporal) throws Exception {
        if (temporal.getTemporalType() != TemporalType.TEMPORAL_INSTANT) {
            throw new Exception("Invalid temporal type.");
        }
        this.temporal = temporal;
        temporalValue = temporal.getSingleTemporalValue(temporal.getValue());
    }

    public TemporalValue<V> getTemporalValue() {
        return temporalValue;
    }

    @Override
    public String toString() {
        return String.format("%s@%s", temporalValue.getValue(),temporalValue.getTime());
    }
}
