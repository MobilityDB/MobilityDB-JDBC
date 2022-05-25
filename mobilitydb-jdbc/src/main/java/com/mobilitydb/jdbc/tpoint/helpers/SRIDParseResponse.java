package com.mobilitydb.jdbc.tpoint.helpers;

public class SRIDParseResponse {
    private int srid;
    private String parsedValue;

    public SRIDParseResponse(int srid, String parsedValue) {
        this.srid = srid;
        this.parsedValue = parsedValue;
    }

    public int getSRID() {
        return srid;
    }

    public String getParsedValue() {
        return parsedValue;
    }
}
