package ly.persona.academic.data.etl;

import ly.persona.academic.data.DataReader;
import ly.persona.academic.data.DataWriter;

import java.util.ArrayList;
import java.util.List;

public interface EtlProcess<T> {
    void process(DataReader<T> reader, DataWriter<T> writer);

    default List<T> processToList(DataReader<T> reader, int maxResults) {
        ArrayList<T> res = new ArrayList<>();
        DataWriter<T> writer = data -> {
            if (res.size() < maxResults) {
                res.add(data);
            }
        };
        try {
            process(reader, writer);
        } finally {
            writer.close();
        }
        return res;
    }
}
