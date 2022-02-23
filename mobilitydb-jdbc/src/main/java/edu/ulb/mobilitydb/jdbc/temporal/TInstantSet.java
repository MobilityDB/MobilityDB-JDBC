package edu.ulb.mobilitydb.jdbc.temporal;

import edu.ulb.mobilitydb.jdbc.core.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.function.Supplier;

public abstract class TInstantSet<V, T extends DataType & TemporalDataType<V>> {
    private T temporalDataType; //tint, tbool
    private List<TemporalValue<V>> temporalValues; //int, bool

    protected TInstantSet(T temporalDataType) throws Exception {
        this.temporalDataType = temporalDataType;
        validateTemporal();
        temporalValues = new ArrayList<>();
        parseValue(temporalDataType.getValue());
    }

    protected TInstantSet(Supplier<? extends T> tConstructor, String value) throws Exception {
        temporalDataType = tConstructor.get();
        temporalDataType.setValue(value);
        temporalValues = new ArrayList<>();
        validateTemporal();
        parseValue(value);
    }

    protected TInstantSet(Supplier<? extends T> tConstructor, String[] values) throws Exception {
        temporalDataType = tConstructor.get();
        temporalValues = new ArrayList<>();
        for (String val : values) {
            temporalValues.add(temporalDataType.getSingleTemporalValue(val.trim()));
        }
        temporalDataType.setValue(buildValue());
        validateTemporal();
    }

    protected TInstantSet(Supplier<? extends T> tConstructor, TInstant<V, T>[] values) throws Exception {
        temporalDataType = tConstructor.get();
        temporalValues = new ArrayList<>();
        for (TInstant<V, T> val : values) {
            temporalValues.add(val.getTemporalValue());
        }
        temporalDataType.setValue(buildValue());
        validateTemporal();
    }

    private void validateTemporal() throws Exception {
        if (temporalDataType.getTemporalType() != TemporalType.TEMPORAL_INSTANT_SET) {
            throw new Exception("Invalid TInstantSet type.");
        }
    }

    private void parseValue(String value){
        String newValue = value.replace("{", "").replace("}", "");
        String[] values = newValue.split(",", -1);
        for (String val : values) {
            temporalValues.add(temporalDataType.getSingleTemporalValue(val.trim()));
        }
    }

    private String buildValue() {
        StringJoiner sj = new StringJoiner(", ");
        for (TemporalValue<V> temp : temporalValues) {
            sj.add(temp.toString());
        }
        return String.format("{%s}", sj.toString());
    }

    private TemporalValue<V> get(int i){
        return temporalValues.get(i);
    }

    @Override
    public boolean equals(Object obj) {
        if ( obj instanceof TInstantSet) {
            TInstantSet fobj = (TInstantSet) obj;

            if (this.temporalValues.size() != fobj.temporalValues.size()) {
                return false;
            }
            for (int i = 0; i < this.temporalValues.size(); i++) {
                TemporalValue<V> thisVal = this.temporalValues.get(i);
                TemporalValue<V> otherVal = fobj.get(i);
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

    public T getDataType() {
        return temporalDataType;
    }

    @Override
    public String toString() {
        return buildValue();
    }
}
