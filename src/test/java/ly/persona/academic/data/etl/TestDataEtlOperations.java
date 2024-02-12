package ly.persona.academic.data.etl;

import java.util.Comparator;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public interface TestDataEtlOperations {
    UnaryOperator<TestData> MAP_FUNCTION = d -> {
        // initial value
        return new TestData(d.getKey(), 1);
    };

    // count duplicates
    BinaryOperator<TestData> REDUCE_FUNCTION = (d1, d2) -> {
        return new TestData(d1.getKey(), d1.getValue() + d2.getValue());
    };

    // leave unique record
    Predicate<TestData> FILTER_FUNCTION = d -> d.getValue() == 1;

    // sort by key
    Comparator<TestData> COMPARATOR = Comparator.comparing(TestData::getKey);
}