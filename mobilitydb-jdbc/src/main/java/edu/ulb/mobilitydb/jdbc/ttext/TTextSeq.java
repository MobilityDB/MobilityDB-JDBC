package edu.ulb.mobilitydb.jdbc.ttext;

import edu.ulb.mobilitydb.jdbc.temporal.TSequence;

import java.sql.SQLException;

public class TTextSeq extends TSequence<String> {
    public TTextSeq(String value) throws SQLException {
        super(value, TText::getSingleTemporalValue);
    }

    public TTextSeq(String[] values) throws SQLException {
        super(values, TText::getSingleTemporalValue);
    }

    public TTextSeq(String[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(values, lowerInclusive, upperInclusive, TText::getSingleTemporalValue);
    }

    public TTextSeq(TTextInst[] values) throws SQLException {
        super(values);
    }

    public TTextSeq(TTextInst[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(values, lowerInclusive, upperInclusive);
    }
}
