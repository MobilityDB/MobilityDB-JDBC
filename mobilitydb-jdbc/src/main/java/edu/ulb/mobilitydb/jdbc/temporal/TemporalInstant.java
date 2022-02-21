package edu.ulb.mobilitydb.jdbc.temporal;

import edu.ulb.mobilitydb.jdbc.core.DataType;

import java.time.OffsetDateTime;
import java.util.function.Supplier;

public abstract class TemporalInstant<V, T extends DataType & TemporalDataType<V>> {
    private T temporalDataType; //tint, tbool
    private TemporalValue<V> temporalValue; //int, bool

    protected TemporalInstant(T temporalDataType) throws Exception {
        this.temporalDataType = temporalDataType;
        validateTemporal();
        temporalValue = temporalDataType.getSingleTemporalValue(temporalDataType.getValue());
    }

    protected TemporalInstant(Supplier<? extends T> tConstructor, String value) throws Exception {
        temporalDataType = tConstructor.get();
        temporalDataType.setValue(value);
        validateTemporal();
        temporalValue = temporalDataType.getSingleTemporalValue(temporalDataType.getValue());
    }

    protected TemporalInstant(Supplier<? extends T> tConstructor, V value, OffsetDateTime time) throws Exception {
        temporalDataType = tConstructor.get();
        temporalValue = new TemporalValue<>(value, time);
        temporalDataType.setValue(temporalValue.toString());
        validateTemporal();
    }

    private void validateTemporal() throws Exception {
        if (temporalDataType.getTemporalType() != TemporalType.TEMPORAL_INSTANT) {
            throw new Exception("Invalid temporal type.");
        }
    }

    @Override
    public boolean equals(Object obj) {
        if ( obj instanceof TemporalInstant) {
            TemporalInstant fobj = (TemporalInstant) obj;

            return this.temporalValue.getValue().equals(fobj.temporalValue.getValue()) &&
                    this.temporalValue.getTime().isEqual(fobj.temporalValue.getTime());
        }
        return false;
    }

    public TemporalValue<V> getTemporalValue() {
        return temporalValue;
    }

    public T getDataType() {
        return temporalDataType;
    }
}
