package edu.ulb.mobilitydb.jdbc;

import edu.ulb.mobilitydb.jdbc.temporal.TemporalType;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

public final class Helper {
    private static final String FORMAT = "yyyy-MM-dd HH:mm:ssX";

    private Helper() {}

    public static OffsetDateTime formatDate(String value) {
        DateTimeFormatter format = DateTimeFormatter.ofPattern(FORMAT);
        return OffsetDateTime.parse(value.trim(), format);
    }

    public static TemporalType getTemporalType(String value, String clazz) throws SQLException {
        if(value.startsWith("Interp=Stepwise;")) {
            if (value.startsWith("{", 16)){
                return TemporalType.TEMPORAL_SEQUENCE_SET;
            } else {
                return TemporalType.TEMPORAL_SEQUENCE;
            }
        } else if (!value.startsWith("{") && !value.startsWith("[") && !value.startsWith("(")) {
            return TemporalType.TEMPORAL_INSTANT;
        } else if (value.startsWith("[") || value.startsWith("(")) {
            return TemporalType.TEMPORAL_SEQUENCE;
        } else if (value.startsWith("{")) {
            if (value.startsWith("[", 1) || value.startsWith("(", 1)) {
                return TemporalType.TEMPORAL_SEQUENCE_SET;
            } else {
                return TemporalType.TEMPORAL_INSTANT_SET;
            }
        } else {
            throw new SQLException(String.format("Could not parse %s value.", clazz));
        }
    }
}
