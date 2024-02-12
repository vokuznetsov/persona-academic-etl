package ly.persona.academic.data.etl.impl.data.processing;

import java.util.LinkedList;
import java.util.List;

public class DataProcessingPipeline<T> {

    private final List<PipelineStep<T>> steps;

    public DataProcessingPipeline() {
        this.steps = new LinkedList<>();
    }

    public void addStep(PipelineStep<T> step) {
        steps.add(step);
    }

    public List<T> executePipelines(List<T> data) {
        for (PipelineStep<T> step : steps) {
            data = step.execute(data);
        }
        return data;
    }

}
