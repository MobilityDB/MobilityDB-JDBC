package edu.ulb.mobilitydb.jdbc.ttext;

import edu.ulb.mobilitydb.jdbc.temporal.TInstantSet;

import java.sql.SQLException;

public class TTextInstSet extends TInstantSet<String> {
    public TTextInstSet(String value) throws SQLException {
        super(value, TText::getSingleTemporalValue);
    }

    public TTextInstSet(String[] values) throws SQLException {
        super(values, TText::getSingleTemporalValue);
    }

    public TTextInstSet(TTextInst[] values) throws SQLException {
        super(values);
    }
}
