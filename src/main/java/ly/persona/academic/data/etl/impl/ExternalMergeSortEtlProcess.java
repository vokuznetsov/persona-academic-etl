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

    @SuppressWarnings("unchecked")
    private void mergeSort(List<File> sortedFiles, DataWriter<T> writer) {
        PriorityQueue<BufferedReader> pq = new PriorityQueue<>((a, b) -> {
            try {

                a.mark(4096);
                b.mark(4096);
                var value =
                    Comparator.nullsLast(comparator)
                        .compare(deserialize(a.readLine()), deserialize(b.readLine()));
                a.reset();
                b.reset();
                return value;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });


        try {
            for (File chunk : sortedFiles) {
                BufferedReader reader = new BufferedReader(new FileReader(chunk));
                pq.add(reader);
            }
            while (!pq.isEmpty()) {
                BufferedReader reader = pq.poll();
                String line = reader.readLine();
                if (line != null) {
                    writer.write(deserialize(line));
                    pq.add(reader);
                } else {
                    reader.close();
                }
            }

            // Step 3: Clean up temporary files
            for (File chunk : sortedFiles) {
                chunk.delete();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
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

        data.sort(comparator);
        if (mapFunction != null)  data = doMap(data, mapFunction);
        if (reduceFunction != null)  data = doReduce(data, reduceFunction);
        if (filterFunction != null)  data = doFilter(data, filterFunction);

        return data;
    }

    private File createFile(int chunkNumber, List<T> data) {
        String fileName = String.format("chunk-%d", chunkNumber);
        try {
            File directory =
                new File("/Users/vladimir_kuznecov/Documents/Code/edu/academic-etl/tmp");
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
