package ly.persona.academic.data.etl.impl.data.processing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;

public class ReducePipelineStep<T> implements PipelineStep<T> {

    private final BinaryOperator<T> reduceFunction;

    public ReducePipelineStep(BinaryOperator<T> reduceFunction) {
        this.reduceFunction = reduceFunction;
    }

    @Override
    public List<T> execute(List<T> data) {
        final Map<T, T> reduceMap = new HashMap<>();
        data.forEach(row -> reduceMap.merge(row, row, reduceFunction));
        return new ArrayList<>(reduceMap.keySet());
    }
}
