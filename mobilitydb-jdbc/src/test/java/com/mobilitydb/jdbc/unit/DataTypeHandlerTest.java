package com.mobilitydb.jdbc.unit;

import com.mobilitydb.jdbc.DataTypeHandler;
import com.mobilitydb.jdbc.boxes.STBox;
import com.mobilitydb.jdbc.boxes.TBox;
import com.mobilitydb.jdbc.tbool.TBool;
import com.mobilitydb.jdbc.tfloat.TFloat;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
import com.mobilitydb.jdbc.time.TimestampSet;
import com.mobilitydb.jdbc.tint.TInt;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPoint;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPoint;
import com.mobilitydb.jdbc.ttext.TText;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;

import java.sql.SQLException;

import static org.mockito.Mockito.*;

class DataTypeHandlerTest {
    @Test
    @DisplayName("Verifying register types works")
    void testRegisterTypes() throws SQLException {
        PGConnection mockedConnection = mock(PGConnection.class);
        DataTypeHandler.INSTANCE.registerTypes(mockedConnection);
        verify(mockedConnection, atLeastOnce()).addDataType("period", Period.class);
        verify(mockedConnection, atLeastOnce()).addDataType("periodset", PeriodSet.class);
        verify(mockedConnection, atLeastOnce()).addDataType("timestampset", TimestampSet.class);
        verify(mockedConnection, atLeastOnce()).addDataType("tbox", TBox.class);
        verify(mockedConnection, atLeastOnce()).addDataType("stbox", STBox.class);
        verify(mockedConnection, atLeastOnce()).addDataType("tint", TInt.class);
        verify(mockedConnection, atLeastOnce()).addDataType("tbool", TBool.class);
        verify(mockedConnection, atLeastOnce()).addDataType("tfloat", TFloat.class);
        verify(mockedConnection, atLeastOnce()).addDataType("ttext", TText.class);
        verify(mockedConnection, atLeastOnce()).addDataType("tgeogpoint", TGeogPoint.class);
        verify(mockedConnection, atLeastOnce()).addDataType("tgeompoint", TGeomPoint.class);
    }
}
