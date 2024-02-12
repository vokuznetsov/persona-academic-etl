package ly.persona.academic.data.etl.impl;

import static ly.persona.academic.data.etl.impl.Utils.serialize;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class FileStorageServiceImpl<T> implements StorageService<T> {


    @Override
    public File createFile(List<T> data) {
        try {
//            File directory =
//                new File("/Users/vladimir_kuznecov/Documents/Code/my/persona-academic-etl/tmp");
            File chunkFile = File.createTempFile("chunk", ".txt");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(chunkFile))) {
                for (T d : data) {
                    writer.write(serialize(d));
                    writer.newLine();
                }
            }
            return chunkFile;
        } catch (IOException e) {
            throw new RuntimeException("Exception occurred while writing data to the file.", e);
        }
    }

    @Override
    public boolean deleteFile(File file) {
        return file.delete();
    }

    @Override
    public void deleteFiles(List<File> files) {
        for (File file : files) {
            deleteFile(file);
        }
    }
}
