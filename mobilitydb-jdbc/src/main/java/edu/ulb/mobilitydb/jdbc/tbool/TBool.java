package edu.ulb.mobilitydb.jdbc.tbool;

import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalDataType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalValue;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

@TypeName(name = "tbool")
public class TBool extends DataType implements TemporalDataType<Boolean> {
    private static final String FORMAT = "yyyy-MM-dd HH:mm:ssX";
    private TemporalType temporalType;

    public TBool() {
        super();
    }

    public TBool(final String value) throws SQLException {
        super();
        setValue(value);
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public void setValue(final String value) throws SQLException {
        if (!value.startsWith("{") && !value.startsWith("[") && !value.startsWith("(")) {
            temporalType = TemporalType.TEMPORAL_INSTANT;
        } else if (value.startsWith("[") || value.startsWith("(")) {
            temporalType = TemporalType.TEMPORAL_SEQUENCE;
        } else if (value.startsWith("{")){
            if (value.startsWith("[",1) || value.startsWith("(",1)) {
                temporalType = TemporalType.TEMPORAL_SEQUENCE_SET;
            } else {
                temporalType = TemporalType.TEMPORAL_INSTANT_SET;
            }
        } else{
            throw new SQLException("Could not parse TBool value.");
        }

        this.value = value;
    }

    @Override
    public TemporalType getTemporalType() {
        return temporalType;
    }

    @Override
    public TemporalValue<Boolean> getSingleTemporalValue(String value) {
        Boolean b;
        DateTimeFormatter format = DateTimeFormatter.ofPattern(FORMAT);
        String[] values = value.trim().split("@");
        OffsetDateTime time = OffsetDateTime.parse(values[1].trim(), format);
        if(values[0].length() == 1) {
            b = values[0].equals("t");
        } else {
            b = Boolean.parseBoolean(values[0]);
        }
        return new TemporalValue<>(b, time);
    }
}