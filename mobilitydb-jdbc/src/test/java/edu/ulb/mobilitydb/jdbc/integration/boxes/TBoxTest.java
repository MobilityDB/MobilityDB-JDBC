package edu.ulb.mobilitydb.jdbc.integration.boxes;

import edu.ulb.mobilitydb.jdbc.boxes.TBox;
import edu.ulb.mobilitydb.jdbc.integration.BaseIntegrationTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;


public class TBoxTest extends BaseIntegrationTest {

    @ParameterizedTest
    @ValueSource(strings = {
        "TBOX((10.0, 2019-09-08 00:00:00+02), (30.0, 2019-09-10 00:00:00+02))",
        "TBOX((, 2019-01-15 00:00:00+02), (, 2019-02-11 00:00:00+02))",
        "TBOX((85.0, ), (86.0, ))"
    })
    void testConstructor(String value) throws SQLException {
        TBox tBox = new TBox(value);

        PreparedStatement insertStatement = con.prepareStatement("INSERT INTO tbl_tbox(boxtype) VALUES (?);");
        insertStatement.setObject(1, tBox);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement("SELECT boxtype FROM tbl_tbox WHERE boxtype=?;");
        readStatement.setObject(1, tBox);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TBox retrievedTBox = (TBox) rs.getObject(1);
            assertEquals(tBox, retrievedTBox);
        } else {
            fail("TBox was not retrieved.");
        }

        readStatement.close();
    }
}
