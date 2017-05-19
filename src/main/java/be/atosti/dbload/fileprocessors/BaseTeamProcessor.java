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
public class BaseTeamProcessor extends FileProcessor {

    private static final String SQL ="INSERT INTO lf_player_has_team(player_playerId, team_teamName) VALUES (?, ?)";

    private BaseTeamProcessor(Path path) {
        this.path = path;
    }

    public static BaseTeamProcessor BaseTeamProcessorFactory(Path path) {
        return new BaseTeamProcessor(path);
    }
    public void load(Connection connection) throws SQLException {
        load(
                BaseTeamProcessor::applyOperations,
                Arrays.asList(
                        new PreparedStatementFiller(connection.prepareStatement(this.SQL), BaseTeamProcessor::fillPreparedStatement)
                ));
    }

    static boolean containsValidHeaders(HashMap<String, String> r) {
        return r.containsKey("player_playerId")
                && r.containsKey("team_teamName");
    }

    static Stream<HashMap<String, String>> applyOperations(Stream<HashMap<String, String>> stream) {
        return stream
                .filter(BaseTeamProcessor::containsValidHeaders);
    }


    static void fillPreparedStatement(PreparedStatement preparedStatement, HashMap<String, String> map) {
        try {
            preparedStatement.setString(1, map.get("player_playerId"));
            preparedStatement.setString(2, map.get("team_teamName"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
