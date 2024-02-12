package ly.persona.academic.data.etl.impl.data.processing;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

public class MapPipelineStep<T> implements PipelineStep<T> {

    private final UnaryOperator<T> mapFunction;

    public MapPipelineStep(UnaryOperator<T> mapFunction) {
        this.mapFunction = mapFunction;
    }

    @Override
    public List<T> execute(List<T> data) {
        final List<T> newData = new ArrayList<>(data.size());
        data.forEach(row -> newData.add(mapFunction.apply(row)));
        return newData;
    }
}
