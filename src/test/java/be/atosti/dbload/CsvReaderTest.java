package be.atosti.dbload;




import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * Created by thomas on 5/7/17.
 */
public class CsvReaderTest {



    @Test
    public void readsRecords() {
        List<HashMap<String,String>> records = readCSV();

        assertEquals("3 record lines in CSV",2,records.size());

        HashMap<String,String> record1 = new HashMap<>();
        record1.put("username","jdoe");
        record1.put("visited","10");

        HashMap<String,String> record2 = new HashMap<>();
        record2.put("username","kolorobot");
        record2.put("visited","4");

        assertTrue("Record should be available",records.contains(record1));
        assertTrue("Record should be available",records.contains(record2));
    }

    private List<HashMap<String,String>> readCSV() {
        Path path = Paths.get("src/test/resources", "sample.csv");

        try (BufferedReader reader = Files.newBufferedReader(
                path, Charset.forName("UTF-8"))){

            CsvReader csvReader = new CsvReader();
            Stream<HashMap<String,String>> streamedRecords = csvReader.readRecords(reader);
            List<HashMap<String,String>> records = streamedRecords.collect(toList());

            return records;

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}