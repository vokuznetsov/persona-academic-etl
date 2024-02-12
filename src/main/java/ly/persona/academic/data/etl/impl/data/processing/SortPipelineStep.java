package ly.persona.academic.data.etl.impl.data.processing;

import java.util.Comparator;
import java.util.List;

public class SortPipelineStep<T> implements PipelineStep<T> {

    private final Comparator<T> comparator;

    public SortPipelineStep(Comparator<T> comparator) {
        this.comparator = comparator;
    }

    @Override
    public List<T> execute(List<T> data) {
        data.sort(comparator);
        return data;
    }
}
