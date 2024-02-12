package ly.persona.academic.data.etl;

import java.util.function.BinaryOperator;
import ly.persona.academic.data.DataReader;
import ly.persona.academic.data.DataWriter;
import ly.persona.academic.data.etl.impl.ExternalMergeSortEtlProcess;
import ly.persona.academic.data.etl.impl.data.processing.DataProcessingPipeline;
import ly.persona.academic.data.etl.impl.data.processing.FilterPipelineStep;
import ly.persona.academic.data.etl.impl.data.processing.MapPipelineStep;
import ly.persona.academic.data.etl.impl.data.processing.ReducePipelineStep;
import ly.persona.academic.data.etl.impl.data.processing.SortPipelineStep;

/**
 * The task is to process the data like SmallDataEtlProcessList (or SmallDataEtlProcessStream) is
 * processing but keep in the memory only restricted number of objects, for example, not more than
 * 100000 elements.
 * <p>
 * It is proposed to implement the data processing using a disk as a storage with linear data
 * writing/reading. An external sorting on files is required for grouping and sorting operation.
 * <p>
 * The solution should be presented as a component(s) that does not depend on TestData directly. To
 * make the code well-designed and reusable it is recommended to use various patterns and techniques
 * like Serializer/Deserializer, Pipeline, Builder, Factory, etc.
 * <p>
 * Place the designed classes in ly.persona.academic.data.etl.impl package, where ReadMe class is
 * located. BigDataEtlProcess class should use those classes and do not process the data directly
 * like example SmallDataEtlProcessList and SmallDataEtlProcessStream classes do.
 */
public class BigDataEtlProcess implements EtlProcess<TestData>, TestDataEtlOperations {


    @Override
    public void process(DataReader<TestData> reader, DataWriter<TestData> writer) {
        BinaryOperator<TestData> reduceFunction = (o1, o2) -> new TestData(o1.getKey(),
            o1.getValue());
        DataProcessingPipeline<TestData> pipeline = new DataProcessingPipeline<>();
        pipeline.addStep(new MapPipelineStep<>(MAP_FUNCTION));
        pipeline.addStep(new ReducePipelineStep<>(reduceFunction));
        pipeline.addStep(new FilterPipelineStep<>(FILTER_FUNCTION));
        pipeline.addStep(new SortPipelineStep<>(COMPARATOR));

        ExternalMergeSortEtlProcess<TestData> fileProcessor = new ExternalMergeSortEtlProcess<>(
            COMPARATOR, reduceFunction, pipeline, TestData.class
        );
        fileProcessor.process(reader, writer);
    }
}
