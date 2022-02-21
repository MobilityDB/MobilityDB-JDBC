package edu.ulb.mobilitydb.jdbc.tint;

import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalDataType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalValue;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

@TypeName(name = "tint")
public class TInt extends DataType implements TemporalDataType<Integer> {
    private static final String FORMAT = "yyyy-MM-dd HH:mm:ssX";
    private TemporalType temporalType;

    public TInt() {
        super();
    }

    public TInt(final String value) throws SQLException {
        super();
        setValue(value);
    }

    @Override
    public String getValue() {
        return value;
    };

    @Override
    public void setValue(final String value) throws SQLException {
        // TODO: Add logic to check the type of TInt
        temporalType = TemporalType.TEMPORAL_INSTANT;
        this.value = value;
    };

    @Override
    public TemporalType getTemporalType() {
        return temporalType;
    }

    @Override
    public TemporalValue<Integer> getSingleTemporalValue(String value) {
        DateTimeFormatter format = DateTimeFormatter.ofPattern(FORMAT);
        String[] values = value.trim().split("@");
        OffsetDateTime time = OffsetDateTime.parse(values[1].trim(), format);
        return new TemporalValue<>(Integer.parseInt(values[0]), time);
    }
}
