package ly.persona.academic.data.test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Comparator;
import java.util.PriorityQueue;
import org.junit.Test;

public class Experiments {

    @Test
    public void name() throws IOException {
        File file1 = new File(
            "/Users/vladimir_kuznecov/Documents/Code/edu/academic-etl/tmp/chunk-02990746675283649054.txt");
        File file2 = new File(
            "/Users/vladimir_kuznecov/Documents/Code/edu/academic-etl/tmp/chunk-14361588648959127512.txt");
        LineNumberReader reader1 = new LineNumberReader(new FileReader(file1));
        LineNumberReader reader2 = new LineNumberReader(new FileReader(file2));

        PriorityQueue<LineNumberReader> pq = new PriorityQueue<>((a, b) -> {
            try {
                a.mark(4096);
                b.mark(4096);
                var value =
                    Comparator.nullsLast(String::compareTo).compare(a.readLine(), b.readLine());
                a.reset();
                b.reset();
                return value;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        pq.add(reader1);
        pq.add(reader2);

        while (!pq.isEmpty()) {
            LineNumberReader reader = pq.poll();
            String line = reader.readLine();
            if (line != null) {
                System.out.println(line);
                pq.add(reader);
            } else {
                reader.close();
            }
        }

    }
}
