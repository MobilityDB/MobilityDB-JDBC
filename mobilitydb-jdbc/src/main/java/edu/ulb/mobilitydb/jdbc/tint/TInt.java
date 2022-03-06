package edu.ulb.mobilitydb.jdbc.tint;

import edu.ulb.mobilitydb.jdbc.core.DateTimeFormatHelper;
import edu.ulb.mobilitydb.jdbc.core.TypeName;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalDataType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalValue;

import java.sql.SQLException;

@TypeName(name = "tint")
public class TInt extends TemporalDataType<Integer> {
    public TInt() {
        super();
    }

    public TInt(final String value) throws SQLException {
        super(value);
    }

    public TInt(Temporal<Integer> temporal) {
        super();
        this.temporal = temporal;
    }

    @Override
    public void setValue(final String value) throws SQLException {
        TemporalType temporalType = TemporalType.getTemporalType(value, this.getClass().getSimpleName());
        switch (temporalType) {
            case TEMPORAL_INSTANT:
                temporal = new TIntInst(value);
                break;
            case TEMPORAL_INSTANT_SET:
                temporal = new TIntInstSet(value);
                break;
            case TEMPORAL_SEQUENCE:
                temporal = new TIntSeq(value);
                break;
            case TEMPORAL_SEQUENCE_SET:
                temporal = new TIntSeqSet(value);
                break;
        }
    }

    public static TemporalValue<Integer> getSingleTemporalValue(String value) {
        String[] values = value.trim().split("@");
        return new TemporalValue<>(Integer.parseInt(values[0]), DateTimeFormatHelper.getDateTimeFormat(values[1]));
    }
}
