package edu.ulb.mobilitydb.jdbc.boxes;

import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

@TypeName(name = "TBox")
public class TBox extends DataType {
    private float xmin;
    private float xmax;
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
        this.xmin = xmin;
        this.xmax = xmax;
        validate();
    }

    public TBox(OffsetDateTime tmin, OffsetDateTime tmax) {
        super();
        this.tmin = tmin;
        this.tmax = tmax;
        validate();
    }

    public TBox(float xmin, OffsetDateTime tmin, float xmax, OffsetDateTime tmax) {
        super();
        this.xmin = xmin;
        this.xmax = xmax;
        this.tmin = tmin;
        this.tmax = tmax;
        validate();
    }


    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public void setValue(String value) throws SQLException {
        value = value.replace("TBOX","")
                .replace("(","")
                .replace(")","");
        String[] values = value.split(",");
        DateTimeFormatter format = DateTimeFormatter.ofPattern(FORMAT);

        if (values.length != 4 ) {
            throw new SQLException("Could not parse TBox value, invalid number of arguments.");
        }
        if(values[0].indexOf('.') != -1) {
            if (values[2].indexOf('.') != -1) {
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


    private void validate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
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
