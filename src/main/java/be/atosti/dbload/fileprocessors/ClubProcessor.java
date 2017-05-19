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
public class ClubProcessor extends FileProcessor {

    private static final String SQL ="INSERT INTO lf_club(clubId, clubName, clubCode,email) values (?, ?, ?, ?)";

    private ClubProcessor(Path path) {
        this.path = path;
    }

    public static ClubProcessor ClubProcessorFactory(Path path) {
        return new ClubProcessor(path);
    }
    public void load(Connection connection) throws SQLException {
        load(
                ClubProcessor::applyOperations,
                Arrays.asList(
                        new PreparedStatementFiller(connection.prepareStatement(this.SQL),ClubProcessor::fillPreparedStatement)
                ));
    }

    static boolean containsValidHeaders(HashMap<String, String> r) {
        return r.containsKey("Nummer")
                && r.containsKey("Naam")
                && r.containsKey("Code")
                && r.containsKey("Email");
    }

    static boolean isValidContent(HashMap<String, String> r) {
        return Utils.isNumeric(r.get("Nummer"));
    }


    static Stream<HashMap<String, String>> applyOperations(Stream<HashMap<String, String>> stream) {
        return stream
                .filter(ClubProcessor::containsValidHeaders)
                .filter(ClubProcessor::isValidContent);
    }


    static void fillPreparedStatement(PreparedStatement preparedStatement, HashMap<String, String> map) {
        try {
            preparedStatement.setString(1, map.get("Nummer"));
            preparedStatement.setString(2, map.get("Naam"));
            preparedStatement.setString(3, map.get("Code"));
            preparedStatement.setString(4, map.get("Email"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
