package be.atosti.dbload.fileprocessors;

import be.atosti.dbload.PreparedStatementFiller;
import be.atosti.dbload.Utils;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Stream;

/**
 * Created by thomas on 5/7/17.
 */
public class LocationsProcessor extends FileProcessor {

    private static final String SQL ="INSERT INTO lf_location(locationId, locationName, address,postalCode,city) VALUES  (?, ?, ?, ?, ?)";

    private LocationsProcessor(Path path) {
        this.path = path;
    }

    public static LocationsProcessor LocationsProcessorFactory(Path path) {
        return new LocationsProcessor(path);
    }
    public void load(Connection connection) throws SQLException {
        load(
                LocationsProcessor::applyOperations,
                Arrays.asList(
                        new PreparedStatementFiller(connection.prepareStatement(this.SQL), LocationsProcessor::fillPreparedStatement)
                ));
    }

    static boolean containsValidHeaders(HashMap<String, String> r) {
        return r.containsKey("code")
                && r.containsKey("name")
                && r.containsKey("address")
                && r.containsKey("postalcode")
                && r.containsKey("city");
    }

    static Stream<HashMap<String, String>> applyOperations(Stream<HashMap<String, String>> stream) {
        return stream
                .filter(LocationsProcessor::containsValidHeaders);
    }


    static void fillPreparedStatement(PreparedStatement preparedStatement, HashMap<String, String> map) {
        try {
            preparedStatement.setString(1, map.get("code"));
            preparedStatement.setString(2, map.get("name"));
            preparedStatement.setString(3, map.get("address"));
            preparedStatement.setString(4, map.get("postalcode"));
            preparedStatement.setString(5, map.get("city"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
