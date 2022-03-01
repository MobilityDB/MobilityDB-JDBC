package edu.ulb.mobilitydb.jdbc.tfloat;

import edu.ulb.mobilitydb.jdbc.Helper;
import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalDataType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalValue;

import java.sql.SQLException;

@TypeName(name = "tfloat")
public class TFloat extends DataType implements TemporalDataType<Float> {
    private TemporalType temporalType;

    public TFloat() { super(); }

    public TFloat(String value) throws SQLException {
        super();
        setValue(value);
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public void setValue(String value) throws SQLException {
        temporalType = Helper.getTemporalType(value, this.getClass().getSimpleName());
        this.value = value;
    }

    @Override
    public TemporalType getTemporalType() {
        return temporalType;
    }

    @Override
    public TemporalValue<Float> getSingleTemporalValue(String value) {
        String[] values = value.trim().split("@");
        return new TemporalValue<>(Float.parseFloat(values[0]), Helper.formatDate(values[1]));
    }
}
