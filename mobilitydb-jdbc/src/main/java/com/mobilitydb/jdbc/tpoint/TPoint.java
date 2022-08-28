package com.mobilitydb.jdbc.tpoint;

import com.mobilitydb.jdbc.core.DateTimeFormatHelper;
import com.mobilitydb.jdbc.temporal.Temporal;
import com.mobilitydb.jdbc.temporal.TemporalDataType;
import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.temporal.TemporalValue;
import com.mobilitydb.jdbc.tpoint.helpers.TPointConstants;
import org.postgis.Geometry;
import org.postgis.GeometryBuilder;
import org.postgis.Point;
import org.postgis.binary.BinaryParser;

import java.sql.SQLException;

/**
 * Base abstract class for TGeomPoint and TGeogPoint
 * Contains logic for handling SRID
 */
public abstract class TPoint extends TemporalDataType<Point> {

    /**
     * The default constructor
     */
    protected TPoint() { super(); }

    /**
     * The string constructor
     * @param value - the string with the TGeomPoint or TGeogPoint value
     * @throws SQLException
     */
    protected TPoint(String value) throws SQLException {
        super(value);
    }


    /**
     * The constructor for temporal types
     * @param temporal - a TGeomPointInst, TGeomPointInstSet, TGeomPointSeq, TGeomPointSeqSet
     *                 TGeogPointInst, TGeogPointInstSet, TGeogPointSeq or a TGeogPointSeqSet
     */
    protected TPoint(Temporal<Point> temporal) {
        super();
        this.temporal = temporal;
    }

    /**
     * Gets the corresponding Temporal type for the string
     * @param value - a string
     * @return a TemporalType
     * @throws SQLException
     */
    protected TemporalType getTemporalType(String value) throws SQLException {
        String newValue = value;

        if (value.startsWith(TPointConstants.SRID_PREFIX)) {
            int idx = value.indexOf(";");
            newValue = value.substring(idx + 1);
        }

        return TemporalType.getTemporalType(newValue, this.getClass().getSimpleName());
    }

    /**
     * Method with compatible signature for delegate
     * {@link com.mobilitydb.jdbc.temporal.delegates.GetSingleTemporalValueFunction}
     * @param value string representation of the value
     * @return Temporal value wrapper with the value parsed
     * @throws SQLException
     */
    public static TemporalValue<Point> getSingleTemporalValue(String value) throws SQLException {
        if (value == null) {
            throw new SQLException("Value cannot be null.");
        }

        String[] values = value.trim().split("@");

        if (values.length != 2) {
            throw new SQLException(String.format("%s is not a valid temporal value.", value));
        }

        // GeometryBuilder doesn't parse correctly if the geometry type is not in upper case
        String type = values[0].toUpperCase();
        Geometry geo = GeometryBuilder.geomFromString(type, new BinaryParser());

        // 1 is POINT
        if (geo.getType() != 1) {
            throw new SQLException(String.format("%s is an invalid Postgis geometry type for temporal point.", type));
        }

        return new TemporalValue<>((Point)geo, DateTimeFormatHelper.getDateTimeFormat(values[1]));
    }

    /**
     * Compares two strings
     * @param first - the first string to compare
     * @param second - the second string to compare
     * @return 0 is both strings are equals, a positive value in case first is greater than second or a negative value
     * if first is less than second
     */
    public static int compareValue(Point first, Point second) {
        throw new UnsupportedOperationException(String.format("Cannot compare points %s , %s", first, second));
    }
}
