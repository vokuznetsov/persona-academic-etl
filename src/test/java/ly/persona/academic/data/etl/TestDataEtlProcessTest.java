package ly.persona.academic.data.etl;

import java.util.ArrayList;
import ly.persona.academic.data.DataGenerator;
import ly.persona.academic.data.DataReader;
import ly.persona.academic.data.TestData;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public final class TestDataEtlProcessTest {
    private final int hashCollisions = 2;
    private final int maxResult = 10;

    @Test
    public void testSmallDataEtlProcessList() {
        EtlProcess<TestData> etl = new SmallDataEtlProcessList();
        doTest(createDataGenerator(100), etl);
    }

    @Test
    public void testSmallDataEtlProcessStream() {
        EtlProcess<TestData> etl = new SmallDataEtlProcessStream();
        doTest(createDataGenerator(100), etl);
    }

    @Test
    public void testBigDataEtlProcess() {
        EtlProcess<TestData> etl = new BigDataEtlProcess();
        doTest(createDataGenerator(1_000_000), etl);
//        doTest(createDataGenerator(101), etl);
    }

    @Test
    public void dataGeneratorTest() {
        DataGenerator<TestData> dataGenerator = createDataGenerator(1000000);
//        DataGenerator<TestData> dataGenerator = createDataGenerator(101);
        List<TestData> list = new ArrayList<>();
        TestData value;
        do {
            value = dataGenerator.read();
            list.add(value);
        } while (value != null);

        System.out.println(list.size());
    }

    private DataGenerator<TestData> createDataGenerator(final int records) {
        if (records < 100) throw new IllegalArgumentException("Too small result records");
        return new DataGenerator<>(
            number -> {
                String key = "key" + number % (records / hashCollisions);
                return new TestData(key, 0);
            },
            records
        );
    }

    private void doTest(DataReader<TestData> reader, EtlProcess<TestData> etl) {
        long time = System.currentTimeMillis();
        // run etl process
        List<TestData> rows = etl.processToList(reader, maxResult);
        time = System.currentTimeMillis() - time;
        System.out.println("Time " + (time / 1000) + " seconds for " + etl.getClass().getSimpleName());

        // check the result is limited
        Assert.assertEquals(maxResult, rows.size());

        List<String> sortedKeys = rows.stream().map(TestData::getKey).sorted().toList();
        for (int i = 0; i < rows.size(); i++) {
            TestData row = rows.get(i);
            // check it is unique record
            Assert.assertEquals(1, row.getValue());
            // check it is sorted
            Assert.assertEquals(sortedKeys.get(i), row.getKey());
        }
    }
}
