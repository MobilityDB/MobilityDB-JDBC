package edu.ulb.mobilitydb.jdbc.time;

import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@TypeName(name = "periodset")
public class PeriodSet extends DataType {
    private final List<Period> periodList;

    public PeriodSet() {
        super();
        periodList = new ArrayList<>();
    }

    public PeriodSet(String value) throws SQLException {
        this();
        setValue(value);
    }

    @Override
    public String getValue() {
        return String.format("{%s}", periodList.stream()
                .map(Period::toString)
                .collect(Collectors.joining(", ")));
    }

    @Override
    public void setValue(String value) throws SQLException {
        String trimmed = value.trim();

        if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
            Matcher m = Pattern.compile("[\\[|\\(].*?[^\\]\\)][\\]|\\)]")
                    .matcher(trimmed);
            while (m.find()) {
                periodList.add(new Period(m.group()));
            }
        } else {
            throw new SQLException("Could not parse period set value.");
        }
    }

    public Period[] getPeriods() {
        return periodList.toArray(new Period[0]);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PeriodSet) {
            PeriodSet fobj = (PeriodSet) obj;

            return periodList.size() == fobj.periodList.size() &&
                    periodList.equals(fobj.periodList);
        }

        return false;
    }

    @Override
    public int hashCode() {
        String value = getValue();
        return value != null ? value.hashCode() : 0;
    }
}
