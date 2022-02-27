package edu.ulb.mobilitydb.jdbc.temporal;

import edu.ulb.mobilitydb.jdbc.core.DataType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class TSequenceSet<V,T extends DataType & TemporalDataType<V>> extends Temporal<V,T> {
    private List<List<TemporalValue<V>>> temporalValues; //int, bool
    private final List<Boolean> lowerInclusive = new ArrayList<>();
    private final List<Boolean> upperInclusive = new ArrayList<>();

    private static final String LOWER_INCLUSIVE = "[";
    private static final String LOWER_EXCLUSIVE = "(";
    private static final String UPPER_INCLUSIVE = "]";
    private static final String UPPER_EXCLUSIVE = ")";

    protected TSequenceSet(T temporalDataType) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE_SET);
        this.temporalDataType = temporalDataType;
        validate();
        temporalValues = new ArrayList<>();
        parseValue(temporalDataType.getValue());
    }

    protected TSequenceSet(Supplier<? extends T> tConstructor, String value) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE_SET);
        temporalDataType = tConstructor.get();
        temporalDataType.setValue(value);
        temporalValues = new ArrayList<>();
        validate();
        parseValue(value);
    }

    protected TSequenceSet(Supplier<? extends T> tConstructor, String[] values) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE_SET);
        temporalDataType = tConstructor.get();
        temporalValues = new ArrayList<>();
        for (String val : values) {
            parseSequence(val);
        }
        temporalDataType.setValue(buildValue());
        validate();
    }

    protected TSequenceSet(Supplier<? extends T> tConstructor, TSequence<V, T>[] values) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE_SET);
        temporalDataType = tConstructor.get();
        temporalValues = new ArrayList<>();
        for (TSequence<V, T> val: values) {
            parseSequence(val.toString());
        }
        temporalDataType.setValue(buildValue());
        validate();
    }


    @Override
    protected void validateTemporalDataType() throws SQLException {
        // TODO: Implement
    }

    @Override
    protected String buildValue() {
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

    private void parseValue(String value) throws SQLException {
        String newValue = value.replace("{", "").replace("}", "").trim();
        Matcher m = Pattern.compile("[\\[|\\(].*?[^\\]\\)][\\]|\\)]")
                .matcher(newValue);
        List<String> seqValues = new ArrayList<>();
        while (m.find()) {
            seqValues.add(m.group());
        }
        for (String seq : seqValues) {
            parseSequence(seq);
        }
    }

    private void parseSequence(String value) throws SQLException {
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
            temp.add(temporalDataType.getSingleTemporalValue(val.trim()));
        }
        temporalValues.add(temp);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() == obj.getClass()) {
            TSequenceSet<V, T> otherTemporal = (TSequenceSet<V, T>) convert(obj);

            if (this.temporalValues.size() != otherTemporal.temporalValues.size()) {
                return false;
            }

            for (int i = 0; i < this.temporalValues.size(); i++) {
                boolean lowerAreEqual = lowerInclusive.get(i) == otherTemporal.lowerInclusive.get(i);
                boolean upperAreEqual = upperInclusive.get(i) == otherTemporal.upperInclusive.get(i);

                if ( this.temporalValues.get(i).size() != otherTemporal.temporalValues.get(i).size()
                        || !lowerAreEqual || ! upperAreEqual) {
                    return false;
                }

                for (int j = 0; j < temporalValues.get(i).size(); j++) {
                    TemporalValue<V> thisVal = this.temporalValues.get(i).get(j);
                    TemporalValue<V> otherVal = otherTemporal.temporalValues.get(i).get(j);
                    boolean valueCheck = thisVal.getValue().equals(otherVal.getValue())
                            && thisVal.getTime().isEqual(otherVal.getTime());
                    if (!valueCheck) {
                        return false;
                    }
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
