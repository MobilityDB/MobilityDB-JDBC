package edu.ulb.mobilitydb.jdbc.ttext;

import edu.ulb.mobilitydb.jdbc.core.DateTimeFormatHelper;
import edu.ulb.mobilitydb.jdbc.core.TypeName;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalDataType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalValue;

import java.sql.SQLException;

@TypeName(name = "ttext")
public class TText extends TemporalDataType<String> {
    public TText() {super();}

    public TText(final String value) throws SQLException {
        super();
        setValue(value);
    }

    public TText(Temporal<String> temporal) {
        super();
        this.temporal = temporal;
    }

    @Override
    public void setValue(final String value) throws SQLException {
        TemporalType temporalType = TemporalType.getTemporalType(value, this.getClass().getSimpleName());
        switch (temporalType) {
            case TEMPORAL_INSTANT:
                temporal = new TTextInst(value);
                break;
            case TEMPORAL_INSTANT_SET:
                temporal = new TTextInstSet(value);
                break;
            case TEMPORAL_SEQUENCE:
                temporal = new TTextSeq(value);
                break;
            case TEMPORAL_SEQUENCE_SET:
                temporal = new TTextSeqSet(value);
                break;
        }
    }

    public static TemporalValue<String> getSingleTemporalValue(String value) {
        String[] values = value.trim().split("@");
        if(values[0].startsWith("\"") && values[0].endsWith("\"")) {
            values[0] = values[0].replace("\"", "");
        }
        return new TemporalValue<>(String.format("%s",values[0]), DateTimeFormatHelper.getDateTimeFormat(values[1]));
    }
}
