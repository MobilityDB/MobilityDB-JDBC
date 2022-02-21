package edu.ulb.mobilitydb.jdbc.temporal;

import edu.ulb.mobilitydb.jdbc.core.DataType;

public class TemporalInstant<V, T extends DataType & TemporalDataType<V>> {
    private T temporal; //tint, tbool
    private TemporalValue<V> temporalValue; //int, bool

    public TemporalInstant(T temporal) throws Exception {
        if (temporal.getTemporalType() != TemporalType.TEMPORAL_INSTANT) {
            throw new Exception("Invalid type");
        }
        this.temporal = temporal;
        temporalValue = temporal.getSingleTemporalValue(temporal.getValue());
    }

    public TemporalValue<V> getTemporalValue() {
        return temporalValue;
    }
}
