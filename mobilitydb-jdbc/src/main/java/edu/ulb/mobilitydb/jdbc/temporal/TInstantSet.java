package edu.ulb.mobilitydb.jdbc.temporal;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public abstract class TInstantSet<V extends Serializable> extends Temporal<V> {
    private final List<TemporalValue<V>> temporalValues = new ArrayList<>(); //int, bool

    protected TInstantSet(String value, GetSingleTemporalValueFunction<V> getSingleTemporalValue) throws SQLException {
        super(TemporalType.TEMPORAL_INSTANT_SET);
        parseValue(value, getSingleTemporalValue);
        validate();
    }

    protected TInstantSet(String[] values, GetSingleTemporalValueFunction<V> getSingleTemporalValue) throws SQLException {
        super(TemporalType.TEMPORAL_INSTANT_SET);
        for (String val : values) {
            temporalValues.add(getSingleTemporalValue.run(val.trim()));
        }
        validate();
    }

    protected TInstantSet(TInstant<V>[] values) throws SQLException {
        super(TemporalType.TEMPORAL_INSTANT_SET);
        for (TInstant<V> val : values) {
            temporalValues.add(val.getTemporalValue());
        }
        validate();
    }

    @Override
    protected void validateTemporalDataType() throws SQLException {
        // TODO: Implement
    }

    @Override
    public String buildValue() {
        StringJoiner sj = new StringJoiner(", ");
        for (TemporalValue<V> temp : temporalValues) {
            sj.add(temp.toString());
        }
        return String.format("{%s}", sj.toString());
    }

    private void parseValue(String value, GetSingleTemporalValueFunction<V> getSingleTemporalValue) throws SQLException {
        String newValue = value.replace("{", "").replace("}", "");
        String[] values = newValue.split(",", -1);
        for (String val : values) {
            temporalValues.add(getSingleTemporalValue.run(val.trim()));
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() == obj.getClass()) {
            TInstantSet<?> otherTemporal = (TInstantSet<?>) obj;
            if (this.temporalValues.size() != otherTemporal.temporalValues.size()) {
                return false;
            }
            for (int i = 0; i < this.temporalValues.size(); i++) {
                TemporalValue<V> thisVal = this.temporalValues.get(i);
                TemporalValue<?> otherVal = otherTemporal.temporalValues.get(i);
                if (!thisVal.equals(otherVal)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        String value = toString();
        return value != null ? value.hashCode() : 0;
    }
}
