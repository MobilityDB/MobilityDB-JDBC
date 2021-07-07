package edu.ulb.mobilitydb.jdbc;

import org.postgresql.util.PGobject;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@TypeName(name = "period")
public class Period extends DataType {
    private String period;
    private Date lower;
    private Date upper;
    private boolean lowerInclusive;
    private boolean upperInclusive;


    /** Instantiate with default state. */
    public Period() {
        super();
    }

    public Period(final String value) throws SQLException {
        this();
        setValue(value);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return period;
    }

    public Date getLower() {
        return lower;
    }

    public Date getUpper() {
        return upper;
    }

    public boolean isLowerInclusive() {
        return lowerInclusive;
    }

    public boolean isUpperInclusive() {
        return upperInclusive;
    }

    /** {@inheritDoc} */
    @Override
    public void setValue(final String value) throws SQLException {
        period = value;
        String[] values = value.split(",");
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssX");
        try {
            this.lower = dateFormat.parse(values[0].substring(1).trim());
            this.lowerInclusive = values[0].substring(0, 1).equals("[");
            this.upper = dateFormat.parse(values[1].substring(0, values[1].length() - 1).trim());
            this.upperInclusive = values[1].endsWith("]");
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
