package com.mobilitydb.jdbc.ttext;

import com.mobilitydb.jdbc.temporal.TSequenceSet;

import java.sql.SQLException;

public class TTextSeqSet extends TSequenceSet<String> {
    public TTextSeqSet(String value) throws SQLException {
        super(value, TText::getSingleTemporalValue);
        stepwise = true;
    }

    public TTextSeqSet(String[] values) throws SQLException {
        super(true, values, TText::getSingleTemporalValue);
    }

    public TTextSeqSet(TTextSeq[] values) throws SQLException {
        super(true, values, TText::getSingleTemporalValue);
    }

    @Override
    protected boolean explicitInterpolation() {
        return false;
    }
}
