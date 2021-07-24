package edu.ulb.mobilitydb.jdbc.unit.boxes;

import edu.ulb.mobilitydb.jdbc.boxes.TBox;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TBoxTest {

    @Test
    void testConstructor() throws SQLException {
        String value = "TBOX((50.0, 2021-07-08 06:04:32+02), (40.0, 2021-07-09 11:02:00+02))";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedTmin = OffsetDateTime.of(2021,7, 8,
                6, 4, 32, 0, tz);
        OffsetDateTime expectedTmax = OffsetDateTime.of(2021, 7, 9,
                11, 2, 0, 0, tz);
        TBox tbox = new TBox(value);
        TBox other = new TBox(50, expectedTmin, 40, expectedTmax);

        assertEquals(expectedTmin, tbox.getTmin());
        assertEquals(expectedTmax, tbox.getTmax());
        assertEquals(other.getValue(), tbox.getValue());
    }

    @Test
    void testConstructorOnlyX() throws SQLException {
        String value = "TBOX((3.0, ), (7.0, ))";
        TBox tbox = new TBox(value);
        TBox other = new TBox(3, 7);

        assertEquals(other.getXmin(), tbox.getXmin());
        assertEquals(other.getXmax(), tbox.getXmax());
        assertEquals(other.getValue(), tbox.getValue());
    }

    @Test
    void testConstructorOnlyT() throws SQLException {
        String value = "TBOX((50.0, 2021-07-08 06:04:32+02), (40.0, 2021-07-09 11:02:00+02))";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedTmin = OffsetDateTime.of(2021,7, 8,
                6, 4, 32, 0, tz);
        OffsetDateTime expectedTmax = OffsetDateTime.of(2021, 7, 9,
                11, 2, 0, 0, tz);
        TBox tbox = new TBox(value);
        TBox other = new TBox(50,40);

        assertEquals(expectedTmin, tbox.getTmin());
        assertEquals(expectedTmax, tbox.getTmax());
        assertEquals(other.getValue(), tbox.getValue());
    }
}
