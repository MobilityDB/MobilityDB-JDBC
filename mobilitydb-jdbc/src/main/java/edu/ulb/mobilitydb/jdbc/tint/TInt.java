package edu.ulb.mobilitydb.jdbc.tint;

import edu.ulb.mobilitydb.jdbc.core.DateTimeFormatHelper;
import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalDataType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalValue;

import java.sql.SQLException;

@TypeName(name = "tint")
public class TInt extends DataType implements TemporalDataType<Integer> {
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
    }

    @Override
    public void setValue(final String value) throws SQLException {
        temporalType = TemporalType.getTemporalType(value, this.getClass().getSimpleName());
        this.value = value;
    }

    @Override
    public TemporalType getTemporalType() {
        return temporalType;
    }

    @Override
    public TemporalValue<Integer> getSingleTemporalValue(String value) throws SQLException {
        String[] values = value.trim().split("@");
        return new TemporalValue<>(Integer.parseInt(values[0]), DateTimeFormatHelper.getDateTimeFormat(values[1]));
    }
}
