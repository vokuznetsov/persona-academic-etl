package ly.persona.academic.data.etl.impl;

import static ly.persona.academic.data.etl.impl.DataProcessor.doFilter;
import static ly.persona.academic.data.etl.impl.DataProcessor.doMap;
import static ly.persona.academic.data.etl.impl.DataProcessor.doReduce;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import ly.persona.academic.data.DataReader;
import ly.persona.academic.data.DataWriter;
import ly.persona.academic.data.TestData;
import ly.persona.academic.data.etl.EtlProcess;

public class ExternalMergeSortEtlProcess<T> implements EtlProcess<T> {

    private static final Integer MAX_FILE_SIZE = 100_000;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Comparator<T> comparator;
    private final UnaryOperator<T> mapFunction;
    private final BinaryOperator<T> reduceFunction;
    private final Predicate<T> filterFunction;

    public ExternalMergeSortEtlProcess(Comparator<T> comparator) {
        this.comparator = comparator;
        this.mapFunction = null;
        this.reduceFunction = null;
        this.filterFunction = null;
    }

    public ExternalMergeSortEtlProcess(Comparator<T> comparator, UnaryOperator<T> mapFunction,
                                       BinaryOperator<T> reduceFunction,
                                       Predicate<T> filterFunction) {
        this.comparator = comparator;
        this.mapFunction = mapFunction;
        this.reduceFunction = reduceFunction;
        this.filterFunction = filterFunction;
    }


    @Override
    public void process(DataReader<T> reader, DataWriter<T> writer) {
        List<File> sortedFiles = new ArrayList<>();

        for (List<T> data = getSortedChunkOfData(reader); !data.isEmpty();
             data = getSortedChunkOfData(reader)) {
            sortedFiles.add(createFile(sortedFiles.size(), data));
        }

        mergeSort(sortedFiles, writer);
    }

    private void mergeSort(List<File> sortedFiles, DataWriter<T> writer) {
        PriorityQueue<Map.Entry<T, BufferedReader>> pq =
            new PriorityQueue<>((a, b) -> comparator.compare(a.getKey(), b.getKey()));

        try {
            for (File chunk : sortedFiles) {
                BufferedReader reader = new BufferedReader(new FileReader(chunk));
                nextLine(pq, reader);
            }
            while (!pq.isEmpty()) {
                Map.Entry<T, BufferedReader> entry = pq.poll();
                T key = entry.getKey();
                nextLine(pq, entry.getValue());

                List<T> mergeKeys = new ArrayList<>();

                while (!pq.isEmpty() && key.equals(pq.peek().getKey())) {
                    Map.Entry<T, BufferedReader> value = pq.poll();
                    nextLine(pq, value.getValue());
                    mergeKeys.add(value.getKey());
                }

                T mergedData = mergeKeys.stream().reduce(key, reduceFunction);
                writer.write(mergedData);
            }

            // Step 3: Clean up temporary files
            for (File chunk : sortedFiles) {
                chunk.delete();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void nextLine(PriorityQueue<Map.Entry<T, BufferedReader>> pq, BufferedReader br)
        throws IOException {
        String line = br.readLine();
        if (line != null) {
            pq.add(Map.entry(deserialize(line), br));
        } else {
            br.close();
        }
    }

    private String serialize(T data) throws JsonProcessingException {
        return objectMapper.writeValueAsString(data);
    }

    private T deserialize(String data) throws JsonProcessingException {
        return data == null ? null : (T) objectMapper.readValue(data, TestData.class);
    }

    private List<T> getSortedChunkOfData(DataReader<T> reader) {
        int count = 0;
        List<T> data = new ArrayList<>();
        for (T row = reader.read(); row != null && count < MAX_FILE_SIZE; row = reader.read()) {
            data.add(row);
            count++;
        }


        if (mapFunction != null) {
            data = doMap(data, mapFunction);
        }
        if (reduceFunction != null) {
            data = doReduce(data, reduceFunction);
        }
        if (filterFunction != null) {
            data = doFilter(data, filterFunction);
        }
        data.sort(comparator);

        return data;
    }

    private File createFile(int chunkNumber, List<T> data) {
        String fileName = String.format("chunk-%d", chunkNumber);
        try {
            File directory =
                new File("/Users/vladimir_kuznecov/Documents/Code/my/persona-academic-etl/tmp");
            File chunkFile = File.createTempFile(fileName, ".txt", directory);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(chunkFile))) {
                for (T d : data) {
                    writer.write(serialize(d));
                    writer.newLine();
                }
            }
            return chunkFile;
        } catch (IOException e) {
            throw new RuntimeException(
                "Exception occurred while writing data to the file (" + fileName + ") .", e);
        }
    }
}
