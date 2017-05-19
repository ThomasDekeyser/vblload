package be.atosti.dbload.fileprocessors;

import be.atosti.dbload.CsvReader;
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
 * Created by thomas on 5/11/17.
 */
public class PlayerProcessorTest {

    @Test
    public void testRowWithHighestPrioityShouldBeSelectedWhenSamePlayerIdIsMultipleTimesInTheCSVFile() {
        List<HashMap<String,String>> records = readCSVAndApplyPlayerOperations();

        assertEquals("2 unique players are found in the CSV",2,records.size());
        assertEquals("Uitgeleende Speler line has highest priority","ArijsUitGeleendeSpeler",
                records.stream()
                .filter(x -> "50183230".equals(x.get("memberid")))
                .findAny()
                .get()
                .get("lastname"));

    }

    private List<HashMap<String,String>> readCSVAndApplyPlayerOperations() {
        Path path = Paths.get("src/test/resources", "playersToTestPlayerPriority.csv");

        try (BufferedReader reader = Files.newBufferedReader(
                path, Charset.forName("UTF-8"))){

            CsvReader csvReader = new CsvReader();
            Stream<HashMap<String,String>> streamedRecords = csvReader.readRecords(reader);
            List<HashMap<String,String>> records= PlayerProcessor.applyOperations(streamedRecords).collect(toList());

            return records;

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
