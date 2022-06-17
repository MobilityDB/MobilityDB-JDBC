package com.mobilitydb.jdbc.tpoint.tgeom;

import com.mobilitydb.jdbc.tpoint.TPointSeqSet;
import com.mobilitydb.jdbc.tpoint.helpers.TPointConstants;

import java.sql.SQLException;

public class TGeomPointSeqSet extends TPointSeqSet {
    public TGeomPointSeqSet(String value) throws SQLException {
        super(value, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeqSet(String[] values) throws SQLException {
        super(TPointConstants.EMPTY_SRID, false, values, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeqSet(boolean stepwise, String[] values) throws SQLException {
        super(TPointConstants.EMPTY_SRID, stepwise, values, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeqSet(TGeomPointSeq[] values) throws SQLException {
        super(TPointConstants.EMPTY_SRID, false, values, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeqSet(boolean stepwise, TGeomPointSeq[] values) throws SQLException {
        super(TPointConstants.EMPTY_SRID, stepwise, values, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeqSet(int srid, String[] values) throws SQLException {
        super(srid, false, values, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeqSet(int srid, boolean stepwise, String[] values) throws SQLException {
        super(srid, stepwise, values, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeqSet(int srid, TGeomPointSeq[] values) throws SQLException {
        super(srid, false, values, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeqSet(int srid, boolean stepwise, TGeomPointSeq[] values) throws SQLException {
        super(srid, stepwise, values, TGeomPoint::getSingleTemporalValue);
    }
}
