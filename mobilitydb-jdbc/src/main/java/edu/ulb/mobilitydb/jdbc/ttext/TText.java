package edu.ulb.mobilitydb.jdbc.ttext;

import edu.ulb.mobilitydb.jdbc.core.DateTimeFormatHelper;
import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalDataType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalValue;

import java.sql.SQLException;

@TypeName(name = "ttext")
public class TText extends DataType implements TemporalDataType<String> {
    private TemporalType temporalType;

    public TText() {super();}

    public TText(final String value) throws SQLException {
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
    public TemporalValue<String> getSingleTemporalValue(String value) throws SQLException {
        String[] values = value.trim().split("@");
        if(values[0].startsWith("\"") && values[0].endsWith("\"")) {
            values[0] = values[0].replace("\"", "");
        }
        return new TemporalValue<>(String.format("%s",values[0]), DateTimeFormatHelper.getDateTimeFormat(values[1]));
    }
}
