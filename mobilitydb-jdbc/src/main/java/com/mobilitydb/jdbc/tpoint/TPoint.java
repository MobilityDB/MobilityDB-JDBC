package com.mobilitydb.jdbc.tpoint;

import com.mobilitydb.jdbc.core.DateTimeFormatHelper;
import com.mobilitydb.jdbc.temporal.Temporal;
import com.mobilitydb.jdbc.temporal.TemporalDataType;
import com.mobilitydb.jdbc.temporal.TemporalValue;
import org.postgis.Geometry;
import org.postgis.GeometryBuilder;
import org.postgis.Point;
import org.postgis.binary.BinaryParser;

import java.sql.SQLException;

public abstract class TPoint extends TemporalDataType<Point> {

    protected TPoint() { super(); }

    protected TPoint(String value) throws SQLException {
        super(value);
    }

    protected TPoint(Temporal<Point> temporal) {
        super();
        this.temporal = temporal;
    }

    public static TemporalValue<Point> getSingleTemporalValue(String value) throws SQLException {
        String[] values = value.trim().split("@");
        // TODO add validations to the string to avoid null exceptions
        // GeometryBuilder doesn't parse correctly if the geometry type is not in upper case
        Geometry geo = GeometryBuilder.geomFromString(values[0].toUpperCase(), new BinaryParser());

        // 1 is point
        if (geo.getType() != 1) {
            // TODO Set correct validation message
            throw new SQLException("Invalid type?");
        }

        return new TemporalValue<>((Point)geo, DateTimeFormatHelper.getDateTimeFormat(values[1]));
    }
}
