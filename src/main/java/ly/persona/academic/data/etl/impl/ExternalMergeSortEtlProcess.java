package ly.persona.academic.data.etl.impl;

import static ly.persona.academic.data.etl.impl.Utils.deserialize;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.BinaryOperator;
import ly.persona.academic.data.DataReader;
import ly.persona.academic.data.DataWriter;
import ly.persona.academic.data.etl.EtlProcess;
import ly.persona.academic.data.etl.impl.data.processing.DataProcessingPipeline;

public class ExternalMergeSortEtlProcess<T> implements EtlProcess<T> {

    private static final Integer MAX_FILE_SIZE = 100_000;

    private final StorageService<T> fileService = new FileStorageServiceImpl<>();
    private final Comparator<T> comparator;
    private final BinaryOperator<T> reduceFunction;
    private final DataProcessingPipeline<T> dataProcessing;
    private final Class<T> clazz;

    public ExternalMergeSortEtlProcess(Comparator<T> comparator, BinaryOperator<T> reduceFunction,
        DataProcessingPipeline<T> pipeline, Class<T> clazz) {
        this.comparator = comparator;
        this.reduceFunction = reduceFunction;
        this.dataProcessing = pipeline;
        this.clazz = clazz;
    }


    @Override
    public void process(DataReader<T> reader, DataWriter<T> writer) {
        List<File> sortedFiles = new ArrayList<>();

        try (reader) {
            for (List<T> data = extractData(reader); !data.isEmpty(); data = extractData(reader)) {
                sortedFiles.add(fileService.createFile(data));
            }
        }

        mergeSort(sortedFiles, writer);
    }

    private void mergeSort(List<File> sortedFiles, DataWriter<T> writer) {
        PriorityQueue<Map.Entry<T, BufferedReader>> pq = new PriorityQueue<>(
            (a, b) -> comparator.compare(a.getKey(), b.getKey()));

        try (writer) {
            for (File chunk : sortedFiles) {
                BufferedReader reader = new BufferedReader(new FileReader(chunk));
                moveToNextRow(pq, reader);
            }
            while (!pq.isEmpty()) {
                Map.Entry<T, BufferedReader> entry = pq.poll();
                moveToNextRow(pq, entry.getValue());

                T row = mergeTheSameKeysFromDifferentStorages(entry.getKey(), pq);
                writer.write(row);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            close(sortedFiles, pq);
        }
    }

    /**
     * This method is necessary to merge the same keys, which are located in different files. For
     * example, file-1 has data {1, 2, 3} and file-2 has data {1, 3, 4} as a result the method merge
     * `1` from both files and return `1` only once.
     */
    private T mergeTheSameKeysFromDifferentStorages(T key,
        PriorityQueue<Map.Entry<T, BufferedReader>> pq) throws IOException {
        if (reduceFunction == null) {
            return key;
        }
        List<T> mergeKeys = new ArrayList<>();

        while (!pq.isEmpty() && key.equals(pq.peek().getKey())) {
            Map.Entry<T, BufferedReader> value = pq.poll();
            moveToNextRow(pq, value.getValue());
            mergeKeys.add(value.getKey());
        }

        return mergeKeys.stream().reduce(key, reduceFunction);
    }

    private void moveToNextRow(PriorityQueue<Map.Entry<T, BufferedReader>> pq, BufferedReader br)
        throws IOException {
        String line = br.readLine();
        if (line != null) {
            pq.add(Map.entry(deserialize(line, clazz), br));
        } else {
            br.close();
        }
    }

    private List<T> extractData(DataReader<T> reader) {
        int count = 0;
        List<T> data = new ArrayList<>();
        for (T row = reader.read(); row != null && count < MAX_FILE_SIZE; row = reader.read()) {
            data.add(row);
            count++;
        }

        return dataProcessing.executePipelines(data);
    }

    private void close(List<File> sortedFiles, PriorityQueue<Map.Entry<T, BufferedReader>> pq) {
        try {
            while (!pq.isEmpty()) {
                pq.poll().getValue().close();
            }
        } catch (IOException e) {
            System.out.println("Exception realising BufferReader: " + e.getMessage());
        }
        fileService.deleteFiles(sortedFiles);
    }
}
