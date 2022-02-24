package edu.ulb.mobilitydb.jdbc.temporal;

import edu.ulb.mobilitydb.jdbc.core.DataType;

import java.time.OffsetDateTime;
import java.util.function.Supplier;

public abstract class TInstant<V, T extends DataType & TemporalDataType<V>> extends Temporal<V, T> {
    private TemporalValue<V> temporalValue; //int, bool

    protected TInstant(T temporalDataType) throws Exception {
        super(TemporalType.TEMPORAL_INSTANT);
        this.temporalDataType = temporalDataType;
        validate();
        temporalValue = temporalDataType.getSingleTemporalValue(temporalDataType.getValue());
    }

    protected TInstant(Supplier<? extends T> tConstructor, String value) throws Exception {
        super(TemporalType.TEMPORAL_INSTANT);
        temporalDataType = tConstructor.get();
        temporalDataType.setValue(value);
        validate();
        temporalValue = temporalDataType.getSingleTemporalValue(temporalDataType.getValue());
    }

    protected TInstant(Supplier<? extends T> tConstructor, V value, OffsetDateTime time) throws Exception {
        super(TemporalType.TEMPORAL_INSTANT);
        temporalDataType = tConstructor.get();
        temporalValue = new TemporalValue<>(value, time);
        temporalDataType.setValue(temporalValue.toString());
        validate();
    }

    @Override
    protected void validateTemporalDataType() throws Exception {
        // TODO: Implement
    }

    @Override
    protected String buildValue() {
        return temporalValue.toString();
    }

    @Override
    protected boolean areEqual(Temporal<V,T> otherTemporal) {
        TInstant<V, T> other = (TInstant<V, T>) otherTemporal;
        return this.temporalValue.getValue().equals(other.temporalValue.getValue()) &&
                this.temporalValue.getTime().isEqual(other.temporalValue.getTime());
    }

    public TemporalValue<V> getTemporalValue() {
        return temporalValue;
    }
}
