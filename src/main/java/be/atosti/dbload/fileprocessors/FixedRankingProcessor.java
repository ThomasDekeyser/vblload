package be.atosti.dbload.fileprocessors;

import be.atosti.dbload.PreparedStatementFiller;
import be.atosti.dbload.Utils;
import org.adrianwalker.multilinestring.Multiline;

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
public class FixedRankingProcessor extends FileProcessor {

    private static final String SQL ="INSERT INTO lf_tmpdbload_15mei ( playerId, playerLevelSingle, playerLevelDouble, playerLevelMixed) VALUES (?,?,?,?)";

    /**
     insert into lf_ranking(date,singles,doubles,mixed,player_playerId)
     select '2000-05-15',t.playerLevelSingle,t.playerLevelDouble,t.playerLevelMixed,t.playerId from lf_tmpdbload_15mei t
     join lf_player p on t.playerId = p.playerId;
     */
    @Multiline
    private static final String INSERT_FIXED_RANKING_FOR_KNOWN_PLAYERS="";

    private FixedRankingProcessor(Path path) {
        this.path = path;
    }

    public static FixedRankingProcessor FixedRankingProcessorFactory(Path path) {
        return new FixedRankingProcessor(path);
    }
    public void load(Connection connection) throws SQLException {
        load(
                FixedRankingProcessor::applyOperations,
                Arrays.asList(
                        new PreparedStatementFiller(connection.prepareStatement(this.SQL), FixedRankingProcessor::fillPreparedStatement)
                ));
        executeSQL(connection,INSERT_FIXED_RANKING_FOR_KNOWN_PLAYERS);
    }

    static boolean containsValidHeaders(HashMap<String, String> r) {
        return r.containsKey("Club")
                && r.containsKey("Lidnummer")
                && r.containsKey("Voornaam")
                && r.containsKey("Achternaam")
                && r.containsKey("Geslacht")
                && r.containsKey("Klassement enkel")
                && r.containsKey("Klassement dubbel")
                && r.containsKey("Klassement gemengd");
    }

    static Stream<HashMap<String, String>> applyOperations(Stream<HashMap<String, String>> stream) {
        return stream
                .filter(FixedRankingProcessor::containsValidHeaders);
    }


    static void fillPreparedStatement(PreparedStatement preparedStatement, HashMap<String, String> map) {
        try {
            preparedStatement.setString(1, map.get("Lidnummer"));
            preparedStatement.setString(2, map.get("Klassement enkel"));
            preparedStatement.setString(3, map.get("Klassement dubbel"));
            preparedStatement.setString(4, map.get("Klassement gemengd"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
