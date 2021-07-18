package edu.ulb.mobilitydb.jdbc.unit.time;

import edu.ulb.mobilitydb.jdbc.time.Period;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class PeriodTest {
    static Stream<Arguments> periodInclusiveProvider() {
        return Stream.of(
            arguments("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01]", true, true),
            arguments("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)", true, false),
            arguments("(2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01]", false, true),
            arguments("(2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)", false, false)
        );
    }

    @Test
    void testConstructor() throws SQLException {
        String value = "[2021-04-08 05:04:45+01, 2021-09-10 10:00:00+01]";
        ZoneOffset tz = ZoneOffset.of("+01:00");
        OffsetDateTime expectedLower = OffsetDateTime.of(2021,4, 8,
                5, 4, 45, 0, tz);
        OffsetDateTime expectedUpper = OffsetDateTime.of(2021, 9, 10,
                10, 0, 0, 0, tz);
        Period period = new Period(value);
        Period other = new Period(expectedLower, expectedUpper, true, true);

        assertEquals(expectedLower, period.getLower());
        assertEquals(expectedUpper, period.getUpper());
        assertEquals(other.getValue(), period.getValue());
    }

    @Test
    void testConstructorDefaultValues() throws SQLException {
        Period period = new Period(OffsetDateTime.now(), OffsetDateTime.now().plusDays(1));

        assertTrue(period.isLowerInclusive());
        assertFalse(period.isUpperInclusive());
    }

    @Test
    void testConstructorValidInstantPeriod() throws SQLException {
        Period period = new Period(OffsetDateTime.now(), OffsetDateTime.now(), true, true);

        assertTrue(period.isLowerInclusive());
        assertTrue(period.isUpperInclusive());
    }

    @ParameterizedTest
    @MethodSource("periodInclusiveProvider")
    void testConstructorInclusiveValues(String value, boolean lowerInclusion, boolean upperInclusion)
            throws SQLException {
        Period period = new Period(value);
        assertAll("Constructor inclusion values",
            () -> assertEquals(lowerInclusion, period.isLowerInclusive()),
            () -> assertEquals(upperInclusion, period.isUpperInclusive()),
            () -> assertEquals(value, period.getValue())
        );
    }

    @Test
    void testConstructorBoundsValidation() {
        SQLException thrown = assertThrows(
            SQLException.class,
            () -> new Period(OffsetDateTime.now().plusDays(1), OffsetDateTime.now())
        );

        assertTrue(thrown.getMessage().contains("The lower bound must be less than or equal to the upper bound"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "[2021-09-08 00:00:00+01, 2021-09-08 00:00:00+01)",
            "(2021-09-08 00:00:00+01, 2021-09-08 00:00:00+01]",
            "(2021-09-08 00:00:00+01, 2021-09-08 00:00:00+01)"
    })
    void testConstructorInstantPeriodBoundsValidation(String value) {
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> new Period(value)
        );
        assertTrue(thrown.getMessage().contains("The lower and upper bounds must be inclusive for an instant period"));
    }

    @ParameterizedTest
    @MethodSource("periodInclusiveProvider")
    void testSetValueBounds(String value, boolean lowerInclusion, boolean upperInclusion)
            throws SQLException {
        Period period = new Period();
        period.setValue(value);
        assertAll("Constructor inclusion values",
                () -> assertEquals(lowerInclusion, period.isLowerInclusive()),
                () -> assertEquals(upperInclusion, period.isUpperInclusive()),
                () -> assertEquals(value, period.getValue())
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "[2021-09-08 00:00:00+01, 2021-09-08 00:00:00+01, 2021-09-08 00:00:00+01)",
            "",
            "a,b,c,d"
    })
    void testSetValueInvalidValue(String value) throws SQLException {
        Period period = new Period();
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> period.setValue(value)
        );
        assertTrue(thrown.getMessage().contains("Could not parse period value"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "]2021-09-08 00:00:00+01, 2021-09-08 00:00:00+01)",
            ")2021-09-08 00:00:00+01, 2021-09-08 00:00:00+01)",
            "{2021-09-08 00:00:00+01, 2021-09-08 00:00:00+01]"
    })
    void testSetValueInvalidLowerBoundFlag(String value) throws SQLException {
        Period period = new Period();
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> period.setValue(value)
        );
        assertTrue(thrown.getMessage().contains("Lower bound flag must be either '[' or '('"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "(2021-09-08 00:00:00+01, 2021-09-08 00:00:00+01(",
            "[2021-09-08 00:00:00+01, 2021-09-08 00:00:00+01[",
            "[2021-09-08 00:00:00+01, 2021-09-08 00:00:00+01{"
    })
    void testSetValueInvalidUpperBoundFlag(String value) throws SQLException {
        Period period = new Period();
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> period.setValue(value)
        );
        assertTrue(thrown.getMessage().contains("Upper bound flag must be either ']' or ')'"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "[2021-09-0800:00:00+01, 2021-09-08 00:00:00+01)",
            "(-09-08 00:00:00+01, 2021-09-08 00:00:00+01]",
            "(2021-09-08 00:00:00+01, 2021-09-08)"
    })
    void testSetValueInvalidDateFormat(String value) throws SQLException {
        Period period = new Period();
        DateTimeParseException thrown = assertThrows(
                DateTimeParseException.class,
                () -> period.setValue(value)
        );
        assertTrue(thrown.getMessage().contains("could not be parsed"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)",
            "(2021-09-07 00:00:00+01, 2021-09-12 00:00:00+01]",
            "(2021-09-06 00:00:00+01, 2021-09-14 00:00:00+01)"
    })
    void testEquals(String value) throws SQLException {
        Period periodA = new Period(value);
        Period periodB = new Period(value);
        assertEquals(periodA, periodB);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)",
            "(2021-09-07 00:00:00+01, 2021-09-12 00:00:00+01]",
            "(2021-09-06 00:00:00+01, 2021-09-14 00:00:00+01)"
    })
    void testNotEquals(String value) throws SQLException {
        Period periodA = new Period(value);
        Period periodB = new Period(periodA.getUpper(), periodA.getUpper().plusSeconds(1),
                periodA.isLowerInclusive(), periodA.isUpperInclusive());
        assertNotEquals(periodA, periodB);
    }

    @Test
    void testEmptyEquals() {
        Period periodA = new Period();
        Period periodB = new Period();
        assertEquals(periodA, periodB);
    }
}
