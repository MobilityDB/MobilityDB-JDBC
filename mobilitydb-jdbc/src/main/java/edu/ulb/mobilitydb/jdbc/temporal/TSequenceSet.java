package edu.ulb.mobilitydb.jdbc.temporal;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class TSequenceSet<V extends Serializable> extends Temporal<V> {
    private List<List<TemporalValue<V>>> temporalValues = new ArrayList<>(); //int, bool
    private final List<Boolean> lowerInclusive = new ArrayList<>();
    private final List<Boolean> upperInclusive = new ArrayList<>();

    private static final String LOWER_INCLUSIVE = "[";
    private static final String LOWER_EXCLUSIVE = "(";
    private static final String UPPER_INCLUSIVE = "]";
    private static final String UPPER_EXCLUSIVE = ")";

    protected TSequenceSet(String value, GetSingleTemporalValueFunction<V> getSingleTemporalValue) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE_SET);
        parseValue(value, getSingleTemporalValue);
        validate();
    }

    protected TSequenceSet(String[] values, GetSingleTemporalValueFunction<V> getSingleTemporalValue)
            throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE_SET);
        for (String val : values) {
            parseSequence(val, getSingleTemporalValue);
        }
        validate();
    }

    protected TSequenceSet(TSequence<V>[] values, GetSingleTemporalValueFunction<V> getSingleTemporalValue)
            throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE_SET);
        for (TSequence<V> val: values) {
            parseSequence(val.toString(), getSingleTemporalValue);
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
        for (int i = 0; i < temporalValues.size(); i++) {
            List<TemporalValue<V>> tempList = temporalValues.get(i);
            StringJoiner sjt = new StringJoiner(", ");
            for (TemporalValue<V> temp : tempList) {
                sjt.add(String.format("%s", temp.toString()));
            }
            sj.add(String.format("%s%s%s",
                    Boolean.TRUE.equals(lowerInclusive.get(i)) ? LOWER_INCLUSIVE : LOWER_EXCLUSIVE,
                    sjt.toString(),
                    Boolean.TRUE.equals(upperInclusive.get(i)) ? UPPER_INCLUSIVE : UPPER_EXCLUSIVE));
        }
        return String.format("{%s}", sj.toString());
    }

    private void parseValue(String value, GetSingleTemporalValueFunction<V> getSingleTemporalValue)
            throws SQLException {
        String newValue = value.replace("{", "").replace("}", "").trim();
        Matcher m = Pattern.compile("[\\[|\\(].*?[^\\]\\)][\\]|\\)]")
                .matcher(newValue);
        List<String> seqValues = new ArrayList<>();
        while (m.find()) {
            seqValues.add(m.group());
        }
        for (String seq : seqValues) {
            parseSequence(seq, getSingleTemporalValue);
        }
    }

    private void parseSequence(String value, GetSingleTemporalValueFunction<V> getSingleTemporalValue)
            throws SQLException {
        String[] values = value.split(",");

        if (values[0].startsWith(LOWER_INCLUSIVE)) {
            this.lowerInclusive.add(true);
        } else if (values[0].startsWith(LOWER_EXCLUSIVE)) {
            this.lowerInclusive.add(false);
        } else {
            throw new SQLException("Lower bound flag must be either '[' or '('.");
        }

        if (values[values.length - 1].endsWith(UPPER_INCLUSIVE)) {
            this.upperInclusive.add(true);
        } else if (values[values.length - 1].endsWith(UPPER_EXCLUSIVE)) {
            this.upperInclusive.add(false);
        } else {
            throw new SQLException("Upper bound flag must be either ']' or ')'.");
        }
        List<TemporalValue<V>> temp = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            String val = values[i];
            if (i == 0) {
                val = val.substring(1);
            }
            if (i == values.length - 1) {
                val = val.substring(0, val.length() - 1);
            }
            temp.add(getSingleTemporalValue.run(val.trim()));
        }
        temporalValues.add(temp);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        TSequenceSet<?> otherTemporal = (TSequenceSet<?>) obj;

        if (this.temporalValues.size() != otherTemporal.temporalValues.size()) {
            return false;
        }

        for (int i = 0; i < this.temporalValues.size(); i++) {
            boolean areEqual = lowerInclusive.get(i) == otherTemporal.lowerInclusive.get(i);
            areEqual = areEqual && upperInclusive.get(i) == otherTemporal.upperInclusive.get(i);
            areEqual = areEqual && temporalValues.get(i).size() == otherTemporal.temporalValues.get(i).size();

            if (!areEqual) {
                return false;
            }

            for (int j = 0; j < temporalValues.get(i).size(); j++) {
                TemporalValue<V> thisVal = temporalValues.get(i).get(j);
                TemporalValue<?> otherVal = otherTemporal.temporalValues.get(i).get(j);
                if (!thisVal.equals(otherVal)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        String value = toString();
        return value != null ? value.hashCode() : 0;
    }
}
