package com.mobilitydb.jdbc.tpoint.tgeom;

import com.mobilitydb.jdbc.tpoint.helpers.TPointConstants;
import com.mobilitydb.jdbc.tpoint.TPointSeq;

import java.sql.SQLException;

public class TGeomPointSeq extends TPointSeq {
    public TGeomPointSeq(String value) throws SQLException {
        super(value, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeq(String[] values) throws SQLException {
        super(TPointConstants.EMPTY_SRID, false, values, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeq(boolean isStepwise, String[] values) throws SQLException {
        super(TPointConstants.EMPTY_SRID, isStepwise, values, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeq(String[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(TPointConstants.EMPTY_SRID,
                false,
                values,
                lowerInclusive,
                upperInclusive,
                TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeq(boolean isStepwise, String[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(TPointConstants.EMPTY_SRID,
                isStepwise,
                values,
                lowerInclusive,
                upperInclusive,
                TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeq(TGeomPointInst[] values) throws SQLException {
        super(TPointConstants.EMPTY_SRID, false, values);
    }

    public TGeomPointSeq(boolean isStepwise, TGeomPointInst[] values) throws SQLException {
        super(TPointConstants.EMPTY_SRID, isStepwise, values);
    }

    public TGeomPointSeq(TGeomPointInst[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(TPointConstants.EMPTY_SRID, false, values, lowerInclusive, upperInclusive);
    }

    public TGeomPointSeq(boolean isStepwise, TGeomPointInst[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(TPointConstants.EMPTY_SRID, isStepwise, values, lowerInclusive, upperInclusive);
    }

    public TGeomPointSeq(int srid, String[] values) throws SQLException {
        super(srid, false, values, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeq(int srid, boolean isStepwise, String[] values) throws SQLException {
        super(srid, isStepwise, values, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeq(int srid, String[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(srid, false, values, lowerInclusive, upperInclusive, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeq(int srid, boolean isStepwise, String[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(srid, isStepwise, values, lowerInclusive, upperInclusive, TGeomPoint::getSingleTemporalValue);
    }

    public TGeomPointSeq(int srid, TGeomPointInst[] values) throws SQLException {
        super(srid, false, values);
    }

    public TGeomPointSeq(int srid, boolean isStepwise, TGeomPointInst[] values) throws SQLException {
        super(srid, isStepwise, values);
    }

    public TGeomPointSeq(int srid, TGeomPointInst[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(srid, false, values, lowerInclusive, upperInclusive);
    }

    public TGeomPointSeq(int srid, boolean isStepwise, TGeomPointInst[] values,
                         boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(srid, isStepwise, values, lowerInclusive, upperInclusive);
    }
}
