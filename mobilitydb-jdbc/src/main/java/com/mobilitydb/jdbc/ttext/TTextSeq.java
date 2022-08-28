package com.mobilitydb.jdbc.ttext;

import com.mobilitydb.jdbc.temporal.TSequence;

import java.sql.SQLException;

/**
 * By Default Interpolation is stepwise
 */
public class TTextSeq extends TSequence<String> {

    /**
     * The string constructor
     * @param value - the string with the TTextSeq value
     * @throws SQLException
     */
    public TTextSeq(String value) throws SQLException {
        super(true, value, TTextInst::new, TText::compareValue);
    }

    /**
     * The string array constructor
     * @param values - an array of strings
     * @throws SQLException
     */
    public TTextSeq(String[] values) throws SQLException {
        super(true, values, TTextInst::new, TText::compareValue);
    }

    /**
     * The string array and bounds constructor
     * @param values - an array of strings
     * @param lowerInclusive - if the lower bound is inclusive
     * @param upperInclusive - if the upper bound is inclusive
     * @throws SQLException
     */
    public TTextSeq(String[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(true, values, lowerInclusive, upperInclusive, TTextInst::new, TText::compareValue);
    }

    /**
     * The TTextInst array constructor
     * @param values - an array of TTextInst
     * @throws SQLException
     */
    public TTextSeq(TTextInst[] values) throws SQLException {
        super(true, values, TText::compareValue);
    }

    /**
     * The TTextInst array and bounds constructor
     * @param values - an array of TTextInst
     * @param lowerInclusive - if the lower bound is inclusive
     * @param upperInclusive - if the upper bound is inclusive
     * @throws SQLException
     */
    public TTextSeq(TTextInst[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(true, values, lowerInclusive, upperInclusive, TText::compareValue);
    }

    @Override
    protected boolean explicitInterpolation() {
        return false;
    }
}
