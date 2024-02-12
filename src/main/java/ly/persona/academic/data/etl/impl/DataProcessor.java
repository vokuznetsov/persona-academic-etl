package ly.persona.academic.data.etl.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public class DataProcessor {

    public static <T> List<T> doMap(List<T> rows, UnaryOperator<T> mapFunction) {
        final List<T> newRows = new ArrayList<>(rows.size());
        rows.forEach(
            row -> newRows.add(mapFunction.apply(row))
        );
        return newRows;
    }

    public static <T> List<T> doReduce(List<T> rows, BinaryOperator<T> reduceFunction) {
        final Map<T, T> reduceMap = new HashMap<>();
        rows.forEach(
            row -> reduceMap.merge(row, row, reduceFunction)
        );
        return new ArrayList<>(reduceMap.keySet());
    }

    public static <T> List<T> doFilter(List<T> rows, Predicate<T> filterFunction) {
        final List<T> newRows = new ArrayList<>(rows.size());
        rows.forEach(
            row -> {
                if (filterFunction.test(row)) {
                    newRows.add(row);
                }
            }
        );
        return newRows;
    }


}
