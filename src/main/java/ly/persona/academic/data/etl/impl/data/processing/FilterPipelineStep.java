package ly.persona.academic.data.etl.impl.data.processing;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class FilterPipelineStep<T> implements PipelineStep<T> {

    private final Predicate<T> filterFunction;

    public FilterPipelineStep(Predicate<T> filterFunction) {
        this.filterFunction = filterFunction;
    }

    @Override
    public List<T> execute(List<T> data) {
        final List<T> newData = new ArrayList<>(data.size());
        data.forEach(
            row -> {
                if (filterFunction.test(row)) {
                    newData.add(row);
                }
            }
        );
        return newData;
    }
}
