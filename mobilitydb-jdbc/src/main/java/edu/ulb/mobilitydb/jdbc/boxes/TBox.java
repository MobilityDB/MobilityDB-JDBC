package edu.ulb.mobilitydb.jdbc.boxes;

import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

@TypeName(name = "tbox")
public class TBox extends DataType {
    private float xmin = 0.0f;
    private float xmax = 0.0f;
    private OffsetDateTime tmin;
    private OffsetDateTime tmax;

    private static final String FORMAT = "yyyy-MM-dd HH:mm:ssX";

    public TBox() {
        super();
    }

    public TBox(final String value) throws SQLException {
        super();
        setValue(value);
    }

    public TBox(float xmin, float xmax) {
        super();
        this.xmin = xmin + 0f;
        this.xmax = xmax + 0f;
    }

    public TBox(OffsetDateTime tmin, OffsetDateTime tmax) {
        super();
        this.tmin = tmin;
        this.tmax = tmax;
    }

    public TBox(float xmin, OffsetDateTime tmin, float xmax, OffsetDateTime tmax) {
        super();
        this.xmin = xmin + 0f;
        this.xmax = xmax + 0f;
        this.tmin = tmin;
        this.tmax = tmax;
    }


    @Override
    public String getValue() {
        DateTimeFormatter format = DateTimeFormatter.ofPattern(FORMAT);
        if (xmin != 0.0f && tmin != null) {
            return String.format("TBOX((%f, %s), (%f, %s))",
                    xmin,
                    format.format(tmin),
                    xmax,
                    format.format(tmax));
        } else if(xmin != 0.0f) {
            return String.format("TBOX((%f, ), (%f, ))", xmin, xmax);
        } else if(tmin != null) {
            return String.format("TBOX((, %s), (, %s))", format.format(tmin), format.format(tmax));
        } else {
            return null;
        }

    }

    @Override
    public void setValue(String value) throws SQLException {
        value = value.replace("TBOX","")
                .replace("(","")
                .replace(")","");
        String[] values = value.split(",", -1);
        DateTimeFormatter format = DateTimeFormatter.ofPattern(FORMAT);

        if (values.length != 4 ) {
            throw new SQLException("Could not parse TBox value, invalid number of arguments.");
        }
        if(values[0].trim().length() > 0) {
            if (values[2].trim().length() > 0) {
                this.xmin = Float.parseFloat(values[0]);
                this.xmax = Float.parseFloat(values[2]);
            } else {
                throw new SQLException("Xmin and xmax should have values.");
            }
        }
        if (values[1].trim().length() > 0) {
            if (values[3].trim().length() > 0) {
                this.tmin = OffsetDateTime.parse(values[1].trim(), format);
                this.tmax = OffsetDateTime.parse(values[3].trim(), format);
            } else {
                throw new SQLException("Tmin and tmax should have values.");
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TBox) {
            TBox fobj = (TBox) obj;

            boolean xminIsEqual = xmin == fobj.getXmin();
            boolean xmaxIsEqual = xmax == fobj.getXmax();

            if (tmin != null && fobj.getTmin() != null) {
                xminIsEqual = xminIsEqual && tmin.isEqual(fobj.getTmin());
            } else {
                xminIsEqual = xminIsEqual && tmin == fobj.getTmin();
            }

            if (tmax != null && fobj.getTmax() != null) {
                xmaxIsEqual = xmaxIsEqual && tmax.isEqual(fobj.getTmax());
            }

            return xminIsEqual && xmaxIsEqual;

        }
        return false;
    }

    @Override
    public int hashCode() {
        String value = getValue();
        return value != null ? value.hashCode() : 0;
    }

    public float getXmin() {
        return xmin;
    }

    public float getXmax() {
        return xmax;
    }

    public OffsetDateTime getTmin() {
        return tmin;
    }

    public OffsetDateTime getTmax() {
        return tmax;
    }
}
