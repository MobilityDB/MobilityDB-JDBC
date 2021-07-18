package edu.ulb.mobilitydb.jdbc.unit.time;

import edu.ulb.mobilitydb.jdbc.time.Period;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class PeriodTest {
    static Stream<Arguments> periodInclusionProvider() {
        return Stream.of(
                arguments("[2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01]", true, true),
                arguments("[2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01)", true, false),
                arguments("(2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01]", false, true),
                arguments("(2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01)", false, false)
        );
    }

    @ParameterizedTest(name = "test")
    @MethodSource("periodInclusionProvider")
    void testConstructorInclusion(String value, boolean lowerInclusion, boolean upperInclusion)
            throws SQLException {
        Period period = new Period(value);
        assertAll("Constructor inclusion values",
            () -> assertEquals(lowerInclusion, period.isLowerInclusive()),
            () -> assertEquals(upperInclusion, period.isUpperInclusive())
        );
    }
}
