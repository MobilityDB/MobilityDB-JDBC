package com.mobilitydb.jdbc.tpoint.tgeog;

import com.mobilitydb.jdbc.temporal.Temporal;
import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tpoint.TPoint;
import com.mobilitydb.jdbc.core.TypeName;
import org.postgis.Point;

import java.sql.SQLException;

@TypeName(name = "tgeogpoint")
public class TGeogPoint extends TPoint {
    public TGeogPoint() { super(); }

    public TGeogPoint(String value) throws SQLException {
        super(value);
    }

    public TGeogPoint(Temporal<Point> temporal) {
        super(temporal);
    }

    @Override
    public void setValue(String value) throws SQLException {
        TemporalType temporalType = TemporalType.getTemporalType(value, this.getClass().getSimpleName());
        switch (temporalType) {
            case TEMPORAL_INSTANT:
                temporal = new TGeogPointInst(value);
                break;
            case TEMPORAL_INSTANT_SET:
                temporal = new TGeogPointInstSet(value);
                break;
            case TEMPORAL_SEQUENCE:
                temporal = new TGeogPointSeq(value);
                break;
            case TEMPORAL_SEQUENCE_SET:
                temporal = new TGeogPointSeqSet(value);
                break;
        }
    }
}
