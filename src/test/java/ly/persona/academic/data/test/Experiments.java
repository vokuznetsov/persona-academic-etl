package ly.persona.academic.data.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.junit.Test;

public class Experiments {

    @Test
    public void test1() throws IOException {
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

    @Test
    public void test2() throws IOException {
        File file1 = new File(
            "/Users/vladimir_kuznecov/Documents/Code/my/persona-academic-etl/tmp/chunk1.txt");
        File file2 = new File(
            "/Users/vladimir_kuznecov/Documents/Code/my/persona-academic-etl/tmp/chunk2.txt");
        BufferedReader reader1 = new BufferedReader(new FileReader(file1));
        BufferedReader reader2 = new BufferedReader(new FileReader(file2));

        PriorityQueue<Map.Entry<String, BufferedReader>> pq = new PriorityQueue<>(
            Map.Entry.comparingByKey()
        );

        String key1 = reader1.readLine();
        String key2 = reader2.readLine();
        if (key1 != null) {
            pq.add(Map.entry(key1, reader1));
        }
        if (key2 != null) {
            pq.add(Map.entry(key2, reader2));
        }

        while (!pq.isEmpty()) {
            Map.Entry<String, BufferedReader> entry = pq.poll();
            String key = entry.getKey();
            nextLine(pq, entry.getValue());

            List<String> keys = new ArrayList<>(List.of(key));

            while (!pq.isEmpty() && key.equals(pq.peek().getKey())) {
                Map.Entry<String, BufferedReader> value = pq.poll();
                nextLine(pq, value.getValue());
                keys.add(value.getKey());
            }

            StringBuffer sb = new StringBuffer();
            keys.forEach(k -> sb.append(k).append(" "));

            System.out.println(sb);
        }
    }

    private void nextLine(PriorityQueue<Map.Entry<String, BufferedReader>> pq, BufferedReader br)
        throws IOException {
        String line = br.readLine();
        if (line != null) {
            pq.add(Map.entry(line, br));
        } else {
            br.close();
        }
    }
}
