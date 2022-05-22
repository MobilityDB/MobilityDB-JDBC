package com.mobilitydb.jdbc.tpoint.tgeog;

import com.mobilitydb.jdbc.tpoint.TPointConstants;
import com.mobilitydb.jdbc.tpoint.TPointInstSet;

import java.sql.SQLException;

public class TGeogPointInstSet extends TPointInstSet {
    public TGeogPointInstSet(String value) throws SQLException {
        super(value, TGeogPoint::getSingleTemporalValue);
    }

    public TGeogPointInstSet(String[] values) throws SQLException {
        super(TPointConstants.DEFAULT_SRID, values, TGeogPoint::getSingleTemporalValue);
    }

    public TGeogPointInstSet(int srid, String[] values) throws SQLException {
        super(srid, values, TGeogPoint::getSingleTemporalValue);
    }

    public TGeogPointInstSet(TGeogPointInst[] values) throws SQLException {
        super(TPointConstants.DEFAULT_SRID, values);
    }

    public TGeogPointInstSet(int srid, TGeogPointInst[] values) throws SQLException {
        super(srid, values);
    }
}
