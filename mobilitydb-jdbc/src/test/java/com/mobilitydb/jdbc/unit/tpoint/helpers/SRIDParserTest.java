package com.mobilitydb.jdbc.unit.tpoint.helpers;

import com.mobilitydb.jdbc.temporal.TemporalValue;
import com.mobilitydb.jdbc.tpoint.helpers.SRIDParseResponse;
import com.mobilitydb.jdbc.tpoint.helpers.SRIDParser;
import com.mobilitydb.jdbc.tpoint.helpers.TPointConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgis.Point;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SRIDParserTest {
    @Test
    void parseSRID_shouldReturnEmptySRIDByDefault() throws SQLException {
        String emptyValue = "";
        SRIDParseResponse response = SRIDParser.parseSRID(emptyValue);
        assertNotNull(response);
        assertEquals(TPointConstants.EMPTY_SRID, response.getSRID());
        assertEquals(emptyValue, response.getParsedValue());
    }

    @Test
    void parseSRID_shouldReturnCorrectSRIDAndRemoveItFromTheValue() throws SQLException {
        String value = "SRID=1234;test";
        SRIDParseResponse response = SRIDParser.parseSRID(value);
        assertNotNull(response);
        assertEquals(1234, response.getSRID());
        assertEquals("test", response.getParsedValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "SRID=-1234;test",
        "SRID=asd;test"
    })
    void parseSRID_shouldThrowErrorIfSRIDIsInvalid(String value) {
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> SRIDParser.parseSRID(value)
        );
        assertEquals("Invalid SRID", thrown.getMessage());
    }

    @Test
    void parseSRID_shouldThrowErrorIfSRIDHasInvalidFormat() {
        String value = "SRID=1234";
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> SRIDParser.parseSRID(value)
        );
        assertEquals("Incorrect format for SRID", thrown.getMessage());
    }

    @Test
    void applySRID_shouldSetTheProvidedSRIDToAllValues() throws SQLException {
        List<TemporalValue<Point>> values = new ArrayList<>();
        values.add(new TemporalValue<>(new Point(1, 1), OffsetDateTime.now()));
        values.add(new TemporalValue<>(new Point(2, 2), OffsetDateTime.now()));
        int srid = 1234;
        int parsedSRID = SRIDParser.applySRID(srid, values);

        assertEquals(srid, parsedSRID);
        assertEquals(srid, values.get(0).getValue().getSrid());
        assertEquals(srid, values.get(1).getValue().getSrid());
    }

    @Test
    void applySRID_shouldUseFirstSRIDIfNotProvided() throws SQLException {
        List<TemporalValue<Point>> values = new ArrayList<>();
        Point firstPoint = new Point(1, 1);
        Point secondPoint = new Point(2, 2);
        int srid = 1234;
        firstPoint.setSrid(srid);
        values.add(new TemporalValue<>(firstPoint, OffsetDateTime.now()));
        values.add(new TemporalValue<>(secondPoint, OffsetDateTime.now()));
        int parsedSRID = SRIDParser.applySRID(srid, values);

        assertEquals(srid, parsedSRID);
        assertEquals(srid, values.get(0).getValue().getSrid());
        assertEquals(srid, values.get(1).getValue().getSrid());
    }

    @Test
    void applySRID_shouldThrowErrorIfSRIDDoesNotMatch() {
        List<TemporalValue<Point>> values = new ArrayList<>();
        int srid = 1234;
        Point point = new Point(1, 1);
        point.setSrid(4321);
        values.add(new TemporalValue<>(point, OffsetDateTime.now()));
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> SRIDParser.applySRID(srid, values)
        );
        assertEquals("Geometry SRID (4321) does not match temporal type SRID (1234)", thrown.getMessage());
    }
}
