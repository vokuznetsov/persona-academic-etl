package ly.persona.academic.data.etl.impl;

import java.io.File;
import java.util.List;

public interface StorageService<T> {

    File createFile(List<T> data);

    boolean deleteFile(File file);

    void deleteFiles(List<File> files);

}
