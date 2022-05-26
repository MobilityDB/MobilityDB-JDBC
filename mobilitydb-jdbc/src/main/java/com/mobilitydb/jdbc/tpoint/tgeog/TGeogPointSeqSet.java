package com.mobilitydb.jdbc.tpoint.tgeog;

import com.mobilitydb.jdbc.tpoint.TPointSeqSet;
import com.mobilitydb.jdbc.tpoint.helpers.TPointConstants;

import java.sql.SQLException;

public class TGeogPointSeqSet extends TPointSeqSet {
    public TGeogPointSeqSet(String value) throws SQLException {
        super(value, TGeogPoint::getSingleTemporalValue);
    }

    public TGeogPointSeqSet(boolean stepwise, String[] values) throws SQLException {
        super(TPointConstants.DEFAULT_SRID, stepwise, values, TGeogPoint::getSingleTemporalValue);
    }

    public TGeogPointSeqSet(boolean stepwise, TGeogPointSeq[] values) throws SQLException {
        super(TPointConstants.DEFAULT_SRID, stepwise, values, TGeogPoint::getSingleTemporalValue);
    }

    public TGeogPointSeqSet(int srid, boolean stepwise, String[] values) throws SQLException {
        super(srid, stepwise, values, TGeogPoint::getSingleTemporalValue);
    }

    public TGeogPointSeqSet(int srid, boolean stepwise, TGeogPointSeq[] values) throws SQLException {
        super(srid, stepwise, values, TGeogPoint::getSingleTemporalValue);
    }
}
