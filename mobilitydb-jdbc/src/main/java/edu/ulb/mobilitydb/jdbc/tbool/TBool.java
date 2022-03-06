package edu.ulb.mobilitydb.jdbc.tbool;

import edu.ulb.mobilitydb.jdbc.core.DateTimeFormatHelper;
import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalDataType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalValue;

import java.sql.SQLException;

@TypeName(name = "tbool")
public class TBool extends DataType implements TemporalDataType<Boolean> {
    private Temporal<Boolean> temporal;

    public TBool() {
        super();
    }

    public TBool(final String value) throws SQLException {
        super();
        setValue(value);
    }

    public TBool(Temporal<Boolean> temporal) throws SQLException {
        super();
        this.temporal = temporal;
    }

    @Override
    public String getValue() {
        return temporal.buildValue();
    }

    @Override
    public void setValue(final String value) throws SQLException {
        TemporalType temporalType = TemporalType.getTemporalType(value, this.getClass().getSimpleName());
        switch (temporalType) {
            case TEMPORAL_INSTANT:
                temporal = new TBoolInst(value);
                break;
            case TEMPORAL_INSTANT_SET:
                temporal = new TBoolInstSet(value);
                break;
            case TEMPORAL_SEQUENCE:
                temporal = new TBoolSeq(value);
                break;
            case TEMPORAL_SEQUENCE_SET:
                temporal = new TBoolSeqSet(value);
                break;
        }
    }

    @Override
    public Temporal<Boolean> getTemporal() {
        return temporal;
    }

    @Override
    public TemporalType getTemporalType() {
        return temporal.getTemporalType();
    }

    public static TemporalValue<Boolean> getSingleTemporalValue(String value) throws SQLException {
        Boolean b;
        String[] values = value.trim().split("@");
        if(values[0].length() == 1) {
            b = values[0].equals("t");
        } else {
            b = Boolean.parseBoolean(values[0]);
        }
        return new TemporalValue<>(b, DateTimeFormatHelper.getDateTimeFormat(values[1]));
    }
}