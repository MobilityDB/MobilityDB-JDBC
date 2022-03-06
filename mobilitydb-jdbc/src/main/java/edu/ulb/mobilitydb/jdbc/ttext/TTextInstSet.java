package edu.ulb.mobilitydb.jdbc.ttext;

import edu.ulb.mobilitydb.jdbc.temporal.TInstantSet;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;

public class TTextInstSet extends TInstantSet<String> {
    public TTextInstSet(String value) throws SQLException {
        super(value, TText::getSingleTemporalValue);
    }

    public TTextInstSet(String[] values) throws SQLException {
        super(values, TText::getSingleTemporalValue);
    }

    //Array of TIntInst
    public TTextInstSet(TTextInst[] values) throws SQLException {
        super(values);
    }

    @Override
    protected Temporal<String> convert(Object obj) {
        if (obj instanceof TTextInstSet) {
            return (TTextInstSet) obj;
        }
        return null;
    }
}
