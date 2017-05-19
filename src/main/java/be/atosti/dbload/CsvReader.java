package be.atosti.dbload;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by thomas on 5/7/17.
 */
public class CsvReader {

    private static final String SEPARATOR = ";";
    public static final String UTF8_BOM = "\uFEFF";


    public Stream<HashMap<String,String>> readRecords(BufferedReader reader ) {
        try{
            String[] headers = reader.readLine().split(SEPARATOR,-1);
            for (int i=0;i<headers.length;i++) {
                headers[i]=removeUTF8BOM(headers[i]);
            }

            return reader.lines()
                    .map(line -> line.split(SEPARATOR,-1))
                    .filter(line -> line.length == headers.length)
                    .map(line -> {
                        HashMap<String,String> r = new HashMap<>();
                        //System.out.println("Processing line"+Arrays.asList(line));
                        for (int i=0;i<headers.length;i++){
                            r.put(headers[i],line[i]);
                        }
                        return r;
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String removeUTF8BOM(String s) {
        if (s.startsWith(UTF8_BOM)) {
            s = s.substring(1);
        }
        return s;
    }
}