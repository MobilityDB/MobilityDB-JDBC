package edu.ulb.mobilitydb.jdbc.temporal;

import edu.ulb.mobilitydb.jdbc.core.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.function.Supplier;

public abstract class TInstantSet<V, T extends DataType & TemporalDataType<V>> extends Temporal<V, T> {
    private List<TemporalValue<V>> temporalValues; //int, bool

    protected TInstantSet(T temporalDataType) throws Exception {
        super(TemporalType.TEMPORAL_INSTANT_SET);
        this.temporalDataType = temporalDataType;
        validate();
        temporalValues = new ArrayList<>();
        parseValue(temporalDataType.getValue());
    }

    protected TInstantSet(Supplier<? extends T> tConstructor, String value) throws Exception {
        super(TemporalType.TEMPORAL_INSTANT_SET);
        temporalDataType = tConstructor.get();
        temporalDataType.setValue(value);
        temporalValues = new ArrayList<>();
        validate();
        parseValue(value);
    }

    protected TInstantSet(Supplier<? extends T> tConstructor, String[] values) throws Exception {
        super(TemporalType.TEMPORAL_INSTANT_SET);
        temporalDataType = tConstructor.get();
        temporalValues = new ArrayList<>();
        for (String val : values) {
            temporalValues.add(temporalDataType.getSingleTemporalValue(val.trim()));
        }
        temporalDataType.setValue(buildValue());
        validate();
    }

    protected TInstantSet(Supplier<? extends T> tConstructor, TInstant<V, T>[] values) throws Exception {
        super(TemporalType.TEMPORAL_INSTANT_SET);
        temporalDataType = tConstructor.get();
        temporalValues = new ArrayList<>();
        for (TInstant<V, T> val : values) {
            temporalValues.add(val.getTemporalValue());
        }
        temporalDataType.setValue(buildValue());
        validate();
    }

    @Override
    protected void validateTemporalDataType() throws Exception {
        // TODO: Implement
    }

    @Override
    protected String buildValue() {
        StringJoiner sj = new StringJoiner(", ");
        for (TemporalValue<V> temp : temporalValues) {
            sj.add(temp.toString());
        }
        return String.format("{%s}", sj.toString());
    }

    private void parseValue(String value){
        String newValue = value.replace("{", "").replace("}", "");
        String[] values = newValue.split(",", -1);
        for (String val : values) {
            temporalValues.add(temporalDataType.getSingleTemporalValue(val.trim()));
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() == obj.getClass()) {
            TInstantSet<V, T> otherTemporal = (TInstantSet<V, T>) convert(obj);
            if (this.temporalValues.size() != otherTemporal.temporalValues.size()) {
                return false;
            }
            for (int i = 0; i < this.temporalValues.size(); i++) {
                TemporalValue<V> thisVal = this.temporalValues.get(i);
                TemporalValue<V> otherVal = otherTemporal.temporalValues.get(i);
                boolean valueCheck = thisVal.getValue().equals(otherVal.getValue())
                        && thisVal.getTime().isEqual(otherVal.getTime());
                if (!valueCheck) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
