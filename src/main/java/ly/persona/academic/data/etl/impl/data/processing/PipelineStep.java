package ly.persona.academic.data.etl.impl.data.processing;

import java.util.List;

public interface PipelineStep<T> {

    List<T> execute(List<T> data);

}
