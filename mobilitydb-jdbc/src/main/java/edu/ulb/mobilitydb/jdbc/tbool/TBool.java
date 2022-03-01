package edu.ulb.mobilitydb.jdbc.tbool;

import edu.ulb.mobilitydb.jdbc.Helper;
import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalDataType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalValue;

import java.sql.SQLException;

@TypeName(name = "tbool")
public class TBool extends DataType implements TemporalDataType<Boolean> {
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
        temporalType = Helper.getTemporalType(value, this.getClass().getSimpleName());
        this.value = value;
    }

    @Override
    public TemporalType getTemporalType() {
        return temporalType;
    }

    @Override
    public TemporalValue<Boolean> getSingleTemporalValue(String value) {
        Boolean b;
        String[] values = value.trim().split("@");
        if(values[0].length() == 1) {
            b = values[0].equals("t");
        } else {
            b = Boolean.parseBoolean(values[0]);
        }
        return new TemporalValue<>(b, Helper.formatDate(values[1]));
    }
}