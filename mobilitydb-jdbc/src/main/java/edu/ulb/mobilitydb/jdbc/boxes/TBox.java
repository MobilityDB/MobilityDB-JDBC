package edu.ulb.mobilitydb.jdbc.boxes;

import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.DateTimeFormatHelper;
import edu.ulb.mobilitydb.jdbc.core.TypeName;

import java.sql.SQLException;
import java.time.OffsetDateTime;

@TypeName(name = "tbox")
public class TBox extends DataType {
    private double xmin = 0.0f;
    private double xmax = 0.0f;
    private OffsetDateTime tmin;
    private OffsetDateTime tmax;

    public TBox() {
        super();
    }

    public TBox(final String value) throws SQLException {
        super();
        setValue(value);
    }

    public TBox(Double xmin, Double xmax) {
        super();
        this.xmin = xmin;
        this.xmax = xmax;
    }

    public TBox(OffsetDateTime tmin, OffsetDateTime tmax) {
        super();
        this.tmin = tmin;
        this.tmax = tmax;
    }

    public TBox(Double xmin, OffsetDateTime tmin, Double xmax, OffsetDateTime tmax) {
        super();
        this.xmin = xmin;
        this.xmax = xmax;
        this.tmin = tmin;
        this.tmax = tmax;
    }


    @Override
    public String getValue() {
        if (xmin != 0.0f && tmin != null) {
            return String.format("TBOX((%f, %s), (%f, %s))",
                    xmin,
                    DateTimeFormatHelper.getStringFormat(tmin),
                    xmax,
                    DateTimeFormatHelper.getStringFormat(tmax));
        } else if(xmin != 0.0f) {
            return String.format("TBOX((%f, ), (%f, ))", xmin, xmax);
        } else if(tmin != null) {
            return String.format("TBOX((, %s), (, %s))", DateTimeFormatHelper.getStringFormat(tmin),
                    DateTimeFormatHelper.getStringFormat(tmax));
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

        if (values.length != 4 ) {
            throw new SQLException("Could not parse TBox value, invalid number of arguments.");
        }
        if(values[0].trim().length() > 0) {
            if (values[2].trim().length() > 0) {
                this.xmin = Double.parseDouble(values[0]);
                this.xmax = Double.parseDouble(values[2]);
            } else {
                throw new SQLException("Xmax should have a value.");
            }
        } else if(values[2].trim().length() > 0) {
            throw new SQLException("Xmin should have a value.");
        }
        if (values[1].trim().length() > 0) {
            if (values[3].trim().length() > 0) {
                this.tmin = DateTimeFormatHelper.getDateTimeFormat(values[1].trim());
                this.tmax = DateTimeFormatHelper.getDateTimeFormat(values[3].trim());
            } else {
                throw new SQLException("Tmax should have a value.");
            }
        } else if(values[3].trim().length() > 0) {
            throw new SQLException("Tmin should have a value.");
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

    public double getXmin() {
        return xmin;
    }

    public double getXmax() {
        return xmax;
    }

    public OffsetDateTime getTmin() {
        return tmin;
    }

    public OffsetDateTime getTmax() {
        return tmax;
    }
}
