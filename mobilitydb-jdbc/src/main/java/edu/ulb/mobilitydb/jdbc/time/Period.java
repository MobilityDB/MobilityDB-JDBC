package edu.ulb.mobilitydb.jdbc.time;

import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

@TypeName(name = "period")
public class Period extends DataType {
    private final DateTimeFormatter format;
    private OffsetDateTime lower;
    private OffsetDateTime upper;
    private boolean lowerInclusive;
    private boolean upperInclusive;

    private static final String LOWER_INCLUSIVE = "[";
    private static final String LOWER_EXCLUSIVE = "(";
    private static final String UPPER_INCLUSIVE = "]";
    private static final String UPPER_EXCLUSIVE = ")";

    /** Instantiate with default state. */
    public Period() {
        super();
        format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssX");
    }

    public Period(final String value) throws SQLException {
        this();
        setValue(value);
    }

    public Period(OffsetDateTime lower, OffsetDateTime upper) throws SQLException {
        this();
        this.lower = lower;
        this.upper = upper;
        this.lowerInclusive = true;
        this.upperInclusive = false;
        validate();
    }

    public Period(OffsetDateTime lower, OffsetDateTime upper, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        this();
        this.lower = lower;
        this.upper = upper;
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
        validate();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return String.format("%s%s,%s%s",
                lowerInclusive ? LOWER_INCLUSIVE : LOWER_EXCLUSIVE,
                format.format(lower),
                format.format(upper),
                upperInclusive ? UPPER_INCLUSIVE : UPPER_EXCLUSIVE);
    }

    public OffsetDateTime getLower() {
        return lower;
    }

    public OffsetDateTime getUpper() {
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
        String[] values = value.split(",");

        if (values.length != 2) {
            throw new SQLException("Could not parse period value");
        }

        if (values[0].startsWith(LOWER_INCLUSIVE)) {
            this.lowerInclusive = true;
        } else if (values[0].startsWith(LOWER_EXCLUSIVE)) {
            this.lowerInclusive = false;
        } else {
            throw new SQLException("Lower bound flag must be either '[' or '('");
        }

        if (values[1].endsWith(UPPER_INCLUSIVE)) {
            this.lowerInclusive = true;
        } else if (values[1].endsWith(UPPER_EXCLUSIVE)) {
            this.lowerInclusive = false;
        } else {
            throw new SQLException("Upper bound flag must be either ']' or ')'");
        }

        this.lower = OffsetDateTime.parse(values[0].substring(1).trim(), format);
        this.upper = OffsetDateTime.parse(values[1].substring(0, values[1].length() - 1).trim(), format);
        validate();
    }

    private void validate() throws SQLException {
        // Are null values valid?
        if (lower == null || upper == null) {
            return;
        }

        if (lower.isAfter(upper)) {
            throw new SQLException("The lower bound must be less than or equal to the upper bound");
        }

        if (lower == upper && (!lowerInclusive || !upperInclusive)) {
            throw new SQLException("The lower and upper bounds must be inclusive for an instant period");
        }
    }
}
