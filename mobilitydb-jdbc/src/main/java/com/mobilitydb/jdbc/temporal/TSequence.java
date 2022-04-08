package com.mobilitydb.jdbc.temporal;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public abstract class TSequence<V extends Serializable> extends Temporal<V> {
    private final List<TemporalValue<V>> temporalValues = new ArrayList<>(); //int, bool
    private boolean lowerInclusive;
    private boolean upperInclusive;

    private static final String LOWER_INCLUSIVE = "[";
    private static final String LOWER_EXCLUSIVE = "(";
    private static final String UPPER_INCLUSIVE = "]";
    private static final String UPPER_EXCLUSIVE = ")";

    protected TSequence(String value, GetSingleTemporalValueFunction<V> getSingleTemporalValue) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE);
        parseValue(value, getSingleTemporalValue);
        validate();
    }

    protected TSequence(String[] values, GetSingleTemporalValueFunction<V> getSingleTemporalValue) throws SQLException {
        this(values, true, false, getSingleTemporalValue);
    }

    protected TSequence(String[] values, boolean lowerInclusive, boolean upperInclusive,
                        GetSingleTemporalValueFunction<V> getSingleTemporalValue) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE);
        for (String val : values) {
            temporalValues.add(getSingleTemporalValue.run(val.trim()));
        }
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
        validate();
    }

    protected TSequence(TInstant<V>[] values) throws SQLException {
        this(values, true, false);
    }

    protected TSequence(TInstant<V>[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE);
        for (TInstant<V> val : values) {
            temporalValues.add(val.getTemporalValue());
        }
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
        validate();
    }

    private void parseValue(String value, GetSingleTemporalValueFunction<V> getSingleTemporalValue)
            throws SQLException {
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
            temporalValues.add(getSingleTemporalValue.run(val.trim()));
        }
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
        return String.format("%s%s%s",
                lowerInclusive ? LOWER_INCLUSIVE : LOWER_EXCLUSIVE,
                sj.toString(),
                upperInclusive ? UPPER_INCLUSIVE : UPPER_EXCLUSIVE);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() == obj.getClass()) {
            TSequence<?> otherTemporal = (TSequence<?>) obj;
            boolean lowerAreEqual = lowerInclusive == otherTemporal.lowerInclusive;
            boolean upperAreEqual = upperInclusive == otherTemporal.upperInclusive;

            if (!lowerAreEqual || ! upperAreEqual) {
                return false;
            }

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
