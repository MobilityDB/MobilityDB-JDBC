package com.mobilitydb.jdbc.tpoint.helpers;

import com.mobilitydb.jdbc.temporal.TemporalValue;
import org.postgis.Point;

import java.sql.SQLException;
import java.util.List;

public class SRIDParser {
    private SRIDParser() { }

    /**
     * Retrieves the initial SRID from the value
     * @param value - Temporal value in string representation
     * @return Temporal value without initial SRID and the SRID value
     * @throws SQLException - When the SRID is invalid
     */
    public static SRIDParseResponse parseSRID(String value) throws SQLException {
        String newString = value;
        int srid = TPointConstants.EMPTY_SRID;

        if (newString.startsWith(TPointConstants.SRID_PREFIX)) {
            int idx = newString.indexOf(";");

            if (idx < 0) {
                throw new SQLException("Incorrect format for SRID");
            }

            String sridString = newString.substring(TPointConstants.SRID_PREFIX.length(), idx);
            newString = newString.substring(idx + 1);

            try {
                srid = Integer.parseInt(sridString);
            } catch (NumberFormatException ex) {
                throw new SQLException("Invalid SRID", ex);
            }

            if (srid < 0) {
                throw new SQLException("Invalid SRID");
            }
        }

        return new SRIDParseResponse(srid, newString);
    }

    /**
     * Applies the SRID to the given temporal values
     * If it is not defined it will use the first defined SRID
     * @param srid - current SRID
     * @param temporalValues - Temporal values
     * @returns the modified SRID
     * @throws SQLException - If any value has a different SRID defined
     */
    public static int applySRID(int srid, List<TemporalValue<Point>> temporalValues) throws SQLException {
        if (srid == TPointConstants.EMPTY_SRID) {
            srid = getFirstSRID(temporalValues);

            if (srid == TPointConstants.EMPTY_SRID) {
                return srid;
            }
        }

        for (TemporalValue<Point> temporalValue : temporalValues) {
            int currentSRID = temporalValue.getValue().getSrid();

            if (currentSRID == TPointConstants.EMPTY_SRID) {
                currentSRID = srid;
                temporalValue.getValue().setSrid(currentSRID);
            }

            if (currentSRID != srid) {
                throw new SQLException(String.format(
                    "Geometry SRID (%d) does not match temporal type SRID (%d)", currentSRID, srid
                ));
            }
        }

        return srid;
    }

    private static int getFirstSRID(List<TemporalValue<Point>> temporalValues) {
        for (TemporalValue<Point> temporalValue : temporalValues) {
            int currentSRID = temporalValue.getValue().getSrid();

            if (currentSRID != TPointConstants.EMPTY_SRID) {
                return currentSRID;
            }
        }

        return TPointConstants.EMPTY_SRID;
    }
}
