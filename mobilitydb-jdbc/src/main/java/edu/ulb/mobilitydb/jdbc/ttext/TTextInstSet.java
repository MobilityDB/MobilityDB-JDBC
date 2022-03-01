package edu.ulb.mobilitydb.jdbc.ttext;

import edu.ulb.mobilitydb.jdbc.temporal.TInstant;
import edu.ulb.mobilitydb.jdbc.temporal.TInstantSet;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;

public class TTextInstSet extends TInstantSet<String, TText> {

    public TTextInstSet(TText temporalDataType) throws SQLException {
        super(temporalDataType);
    }

    public TTextInstSet(String value) throws SQLException {
        super(TText::new, value);
    }

    public TTextInstSet(String[] values) throws SQLException {
        super(TText::new, values);
    }

    //Array of TIntInst
    public TTextInstSet(TTextInst[] values) throws SQLException {
        super(TText::new, values);
    }

    @Override
    protected Temporal<String, TText> convert(Object obj) {
        if (obj instanceof TTextInstSet) {
            return (TTextInstSet) obj;
        }
        return null;
    }
}
