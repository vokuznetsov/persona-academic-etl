package ly.persona.academic.data.etl;

import ly.persona.academic.data.DataReader;
import ly.persona.academic.data.DataWriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import ly.persona.academic.data.TestData;
import org.junit.Test;

/**
 * Keeps all records in the memory.
 * Note: This is example only. This code depends on a TestData class and can't be really used.
 */
public class SmallDataEtlProcessList implements EtlProcess<TestData>, TestDataEtlOperations {
    
    @Override
    public void process(DataReader<TestData> reader, DataWriter<TestData> writer) {
        List<TestData> rows = new ArrayList<>();
        for (TestData row = reader.read(); row != null; row = reader.read()) {
            rows.add(row);
        }
        rows = doMap(rows);
        rows = doReduce(rows);
        rows = doFilter(rows);
        rows.sort(COMPARATOR);
        // write result
        rows.forEach(row -> writer.write(row));
    }

    private List<TestData> doMap(List<TestData> rows) {
        final List<TestData> newRows = new ArrayList<>(rows.size());
        rows.forEach(
                row -> newRows.add(MAP_FUNCTION.apply(row))
        );
        return newRows;
    }
    
    private List<TestData> doReduce(List<TestData> rows) {
        final Map<TestData, TestData> reduceMap = new HashMap<>();
        rows.forEach(
                row -> reduceMap.merge(row, row, REDUCE_FUNCTION)
        );
        return new ArrayList<>(reduceMap.keySet());
    }

    private List<TestData> doFilter(List<TestData> rows) {
        final List<TestData> newRows = new ArrayList<>(rows.size());
        rows.forEach(
                row -> {
                    if (FILTER_FUNCTION.test(row)) {
                        newRows.add(row);
                    }
                }
        );
        return newRows;
    }
}

