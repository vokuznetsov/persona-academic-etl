package ly.persona.academic.data.etl;

import ly.persona.academic.data.DataReader;
import ly.persona.academic.data.DataWriter;

import java.util.HashMap;
import java.util.stream.Stream;
import ly.persona.academic.data.TestData;

/**
 * Keeps all records in the memory.
 * Note: This is example only. This code depends on a TestData class and can't be really used.
 */
public class SmallDataEtlProcessStream implements EtlProcess<TestData>, TestDataEtlOperations {

    @Override
    public void process(DataReader<TestData> reader, DataWriter<TestData> writer) {
        Stream.iterate(reader.read(), rec -> rec != null, rec -> reader.read())
            .map(MAP_FUNCTION)
            .reduce(new HashMap<TestData, TestData>(),
                    (map, obj) -> {
                        map.merge(obj, obj, (o1, o2) -> REDUCE_FUNCTION.apply(o1, o2));
                        return map;
                    },
                    (m, m2) -> {
                        m.putAll(m2);
                        return m;
                    }
            )
            .keySet()
            .stream()
            .filter(FILTER_FUNCTION)
            .sorted(COMPARATOR)
            .forEach(
                    row -> writer.write(row)
            );
    }
}

