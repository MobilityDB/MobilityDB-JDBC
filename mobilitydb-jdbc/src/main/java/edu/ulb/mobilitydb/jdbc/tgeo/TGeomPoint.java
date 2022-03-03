package edu.ulb.mobilitydb.jdbc.tgeo;

import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.DateTimeFormatHelper;
import edu.ulb.mobilitydb.jdbc.core.TypeName;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalDataType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalType;
import edu.ulb.mobilitydb.jdbc.temporal.TemporalValue;
import org.postgis.Geometry;
import org.postgis.GeometryBuilder;
import org.postgis.Point;
import org.postgis.binary.BinaryParser;

import java.sql.SQLException;

@TypeName(name = "tgeompoint")
public class TGeomPoint extends DataType implements TemporalDataType<Point> {
    private TemporalType temporalType;

    public TGeomPoint() { super(); }

    public TGeomPoint(String value) throws SQLException {
        super();
        setValue(value);
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public void setValue(String value) throws SQLException {
        temporalType = TemporalType.getTemporalType(value, this.getClass().getSimpleName());
        this.value = value;
    }

    @Override
    public TemporalType getTemporalType() {
        return temporalType;
    }

    @Override
    public TemporalValue<Point> getSingleTemporalValue(String value) throws SQLException {
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
