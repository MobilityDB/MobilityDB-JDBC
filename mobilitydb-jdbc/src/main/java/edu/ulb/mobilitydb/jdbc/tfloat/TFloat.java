package edu.ulb.mobilitydb.jdbc.tfloat;

import edu.ulb.mobilitydb.jdbc.core.DateTimeFormatHelper;
import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalDataType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalValue;

import java.sql.SQLException;

@TypeName(name = "tfloat")
public class TFloat extends DataType implements TemporalDataType<Float> {
    private Temporal<Float> temporal;

    public TFloat() { super(); }

    public TFloat(String value) throws SQLException {
        super();
        setValue(value);
    }

    public TFloat(Temporal<Float> temporal) throws SQLException {
        super();
        this.temporal = temporal;
    }

    @Override
    public String getValue() {
        return temporal.buildValue();
    }

    @Override
    public void setValue(String value) throws SQLException {
        TemporalType temporalType = TemporalType.getTemporalType(value, this.getClass().getSimpleName());
        switch (temporalType) {
            case TEMPORAL_INSTANT:
                temporal = new TFloatInst(value);
                break;
            case TEMPORAL_INSTANT_SET:
                temporal = new TFloatInstSet(value);
                break;
            case TEMPORAL_SEQUENCE:
                temporal = new TFloatSeq(value);
                break;
            case TEMPORAL_SEQUENCE_SET:
                temporal = new TFloatSeqSet(value);
                break;
        }
    }

    @Override
    public Temporal<Float> getTemporal() {
        return temporal;
    }

    @Override
    public TemporalType getTemporalType() {
        return temporal.getTemporalType();
    }

    public static TemporalValue<Float> getSingleTemporalValue(String value) throws SQLException {
        String[] values = value.trim().split("@");
        return new TemporalValue<>(Float.parseFloat(values[0]), DateTimeFormatHelper.getDateTimeFormat(values[1]));
    }
}
