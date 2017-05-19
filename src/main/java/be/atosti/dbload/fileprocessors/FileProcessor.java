package be.atosti.dbload.fileprocessors;

import be.atosti.dbload.CsvReader;
import be.atosti.dbload.PreparedStatementFiller;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/**
 * Created by thomas on 5/7/17.
 */
public class FileProcessor {
    protected Path path;

    int count = 0;
    final int batchSize = 1000;



    public void load(UnaryOperator<Stream<HashMap<String, String>>> operations,
                     List<PreparedStatementFiller> preparedStatementFillers) {

        try (BufferedReader reader = Files.newBufferedReader(
                this.path, Charset.forName("UTF-8"))) {

            CsvReader csvReader = new CsvReader();


            operations.apply(csvReader.readRecords(reader))
                    .sequential()
                    .forEach(r -> {
                        preparedStatementFillers.forEach(preparedStatementFiller -> {
                                preparedStatementFiller.getPreparedStatementHashMapBiConsumer().accept(preparedStatementFiller.getPreparedStatement(),r);
                                    try {
                                        preparedStatementFiller.getPreparedStatement().addBatch();
                                    } catch (SQLException e) {
                                        e.printStackTrace();
                                    }
                                }
                        );

                        //execute prepared statement in batch
                        if (++count % batchSize == 0) {
                            preparedStatementFillers.forEach(preparedStatementFiller -> {
                                try {
                                    preparedStatementFiller.getPreparedStatement().executeBatch();
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                }
                            });
                        }
                    });

            //Execute last batches and close the PreparedStatements
            preparedStatementFillers.forEach(preparedStatementFiller -> {
                try {
                    preparedStatementFiller.getPreparedStatement().executeBatch();
                    preparedStatementFiller.getPreparedStatement().close();

                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void executeSQL(Connection connection,String sql) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(sql);
        try{
            ps.execute();
        }finally {
            ps.close();
        }
    }


}
