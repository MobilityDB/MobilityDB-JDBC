package edu.ulb.mobilitydb.jdbc.temporal;

import java.sql.SQLException;
import java.time.OffsetDateTime;

public abstract class TInstant<V> extends Temporal<V> {
    private final TemporalValue<V> temporalValue; //int, bool

    protected TInstant(String value, GetSingleTemporalValueFunction<V> getSingleTemporalValue) throws SQLException {
        super(TemporalType.TEMPORAL_INSTANT);
        temporalValue = getSingleTemporalValue.run(value);
        validate();
    }

    protected TInstant(V value, OffsetDateTime time) throws SQLException {
        super(TemporalType.TEMPORAL_INSTANT);
        temporalValue = new TemporalValue<>(value, time);
        validate();
    }

    @Override
    protected void validateTemporalDataType() throws SQLException {
        // TODO: Implement
    }

    @Override
    public String buildValue() {
        return temporalValue.toString();
    }

    public TemporalValue<V> getTemporalValue() {
        return temporalValue;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() == obj.getClass()) {
            TInstant<V> otherTemporal = (TInstant<V>) convert(obj);
            return this.temporalValue.getValue().equals(otherTemporal.temporalValue.getValue()) &&
                    this.temporalValue.getTime().isEqual(otherTemporal.temporalValue.getTime());
        }
        return false;
    }

    @Override
    public int hashCode() {
        String value = toString();
        return value != null ? value.hashCode() : 0;
    }
}
