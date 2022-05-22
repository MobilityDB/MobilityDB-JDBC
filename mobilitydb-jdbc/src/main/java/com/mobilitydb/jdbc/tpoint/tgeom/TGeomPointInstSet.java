package com.mobilitydb.jdbc.tpoint.tgeom;

import com.mobilitydb.jdbc.tpoint.TPointConstants;
import com.mobilitydb.jdbc.tpoint.TPointInstSet;

import java.sql.SQLException;

public class TGeomPointInstSet extends TPointInstSet {
    public TGeomPointInstSet(String value) throws SQLException {
        super(value, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointInstSet(String[] values) throws SQLException {
        super(TPointConstants.EMPTY_SRID, values, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointInstSet(int srid, String[] values) throws SQLException {
        super(srid, values, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointInstSet(TGeomPointInst[] values) throws SQLException {
        super(TPointConstants.EMPTY_SRID, values);
    }

    public TGeomPointInstSet(int srid, TGeomPointInst[] values) throws SQLException {
        super(srid, values);
    }
}
