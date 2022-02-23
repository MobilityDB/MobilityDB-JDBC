package edu.ulb.mobilitydb.jdbc.temporal;

import edu.ulb.mobilitydb.jdbc.core.DataType;

import java.sql.SQLException;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.function.Supplier;

public class TSequence<V,T extends DataType & TemporalDataType<V>>{
    private T temporalDataType; //tint, tbool
    private List<TemporalValue<V>> temporalValues; //int, bool
    private boolean lowerInclusive;
    private boolean upperInclusive;

    private static final String LOWER_INCLUSIVE = "[";
    private static final String LOWER_EXCLUSIVE = "(";
    private static final String UPPER_INCLUSIVE = "]";
    private static final String UPPER_EXCLUSIVE = ")";

    protected TSequence(T temporalDataType) throws Exception {
        this.temporalDataType = temporalDataType;
        validateTemporal();
        temporalValues = new ArrayList<>();
        parseValue(temporalDataType.getValue());
    }

    protected TSequence(Supplier<? extends T> tConstructor, String value) throws Exception {
        temporalDataType = tConstructor.get();
        temporalDataType.setValue(value);
        temporalValues = new ArrayList<>();
        validateTemporal();
        parseValue(value);
    }

    protected TSequence(Supplier<? extends T> tConstructor, String[] values) throws Exception {
        temporalDataType = tConstructor.get();
        temporalValues = new ArrayList<>();
        for (String val : values) {
            temporalValues.add(temporalDataType.getSingleTemporalValue(val.trim()));
        }
        this.lowerInclusive = true;
        this.upperInclusive = false;
        temporalDataType.setValue(buildValue());
        validateTemporal();
    }

    protected TSequence(Supplier<? extends T> tConstructor, String[] values, boolean lowerInclusive,
            boolean upperInclusive) throws Exception {
        temporalDataType = tConstructor.get();
        temporalValues = new ArrayList<>();
        for (String val : values) {
            temporalValues.add(temporalDataType.getSingleTemporalValue(val.trim()));
        }
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
        temporalDataType.setValue(buildValue());
        validateTemporal();
    }

    protected TSequence(Supplier<? extends T> tConstructor, TInstant<V, T>[] values) throws Exception {
        temporalDataType = tConstructor.get();
        temporalValues = new ArrayList<>();
        for (TInstant<V, T> val : values) {
            temporalValues.add(val.getTemporalValue());
        }
        this.lowerInclusive = true;
        this.upperInclusive = false;
        temporalDataType.setValue(buildValue());
        validateTemporal();
    }

    protected TSequence(Supplier<? extends T> tConstructor, TInstant<V, T>[] values, boolean lowerInclusive,
                        boolean upperInclusive) throws Exception {
        temporalDataType = tConstructor.get();
        temporalValues = new ArrayList<>();
        for (TInstant<V, T> val : values) {
            temporalValues.add(val.getTemporalValue());
        }
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
        temporalDataType.setValue(buildValue());
        validateTemporal();
    }

    private void validateTemporal() throws Exception {
        if (temporalDataType.getTemporalType() != TemporalType.TEMPORAL_SEQUENCE) {
            throw new Exception("Invalid TSequence type.");
        }
    }

    private void parseValue(String value) throws SQLException {
        String[] values = value.split(",");

        if (values[0].startsWith(LOWER_INCLUSIVE)) {
            this.lowerInclusive = true;
        } else if (values[0].startsWith(LOWER_EXCLUSIVE)) {
            this.lowerInclusive = false;
        } else {
            throw new SQLException("Lower bound flag must be either '[' or '('.");
        }

        if (values[values.length - 1].endsWith(UPPER_INCLUSIVE)) {
            this.upperInclusive = true;
        } else if (values[values.length - 1].endsWith(UPPER_EXCLUSIVE)) {
            this.upperInclusive = false;
        } else {
            throw new SQLException("Upper bound flag must be either ']' or ')'.");
        }

        for (int i = 0; i < values.length; i++) {
            String val = values[i];
            if (i == 0) {
                val = val.substring(1);
            }
            if (i == values.length - 1 ) {
                val = val.substring(0, val.length() - 1);
            }
            temporalValues.add(temporalDataType.getSingleTemporalValue(val.trim()));
        }
    }

    private String buildValue() {
        StringJoiner sj = new StringJoiner(", ");
        for (TemporalValue<V> temp : temporalValues) {
            sj.add(temp.toString());
        }
        return String.format("%s%s%s",
                lowerInclusive ? LOWER_INCLUSIVE : LOWER_EXCLUSIVE,
                sj.toString(),
                upperInclusive ? UPPER_INCLUSIVE : UPPER_EXCLUSIVE);
    }

    private TemporalValue<V> get(int i){
        return temporalValues.get(i);
    }

    @Override
    public boolean equals(Object obj) {
        if ( obj instanceof TSequence) {
            TSequence fobj = (TSequence) obj;

            boolean lowerAreEqual = lowerInclusive == fobj.lowerInclusive;
            boolean upperAreEqual = upperInclusive == fobj.upperInclusive;

            if (!lowerAreEqual || ! upperAreEqual) {
                return false;
            }

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
