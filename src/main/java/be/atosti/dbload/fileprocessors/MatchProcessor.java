package be.atosti.dbload.fileprocessors;

import be.atosti.dbload.PreparedStatementFiller;
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
public class MatchProcessor extends FileProcessor {


    /**
     INSERT INTO lf_match(homeTeamName, outTeamName, locationId, locationName, matchId, date)
     VALUES (?, ?, ?, ?, ?, str_to_date(?, '%e-%c-%Y %H:%i:%S'))
     */
    @Multiline
    private static final String SQL_INSERT_MATCH ="";


    /**
     INSERT INTO lf_match_extra(oTeamName,hTeamName)
     SELECT m.outTeamName,m.homeTeamName FROM lf_match m
     left join lf_match_extra e on e.oTeamName =m.outTeamName and e.hTeamName = m.homeTeamName
     where e.matchIdExtra is null
     */
    @Multiline
    private static final String INSERT_MATCH_EXTRA="";

    private MatchProcessor(Path path) {
        this.path = path;
    }

    public static MatchProcessor MatchProcessorFactory(Path path) {
        return new MatchProcessor(path);
    }
    public void load(Connection connection) throws SQLException {
        load(
                MatchProcessor::applyOperations,
                Arrays.asList(
                        new PreparedStatementFiller(connection.prepareStatement(this.SQL_INSERT_MATCH),MatchProcessor::fillPreparedStatement)
                ));

        executeSQL(connection,INSERT_MATCH_EXTRA);
    }

    static boolean containsValidHeaders(HashMap<String, String> r) {
        return r.containsKey("matchid")
                && r.containsKey("eventid")
                && r.containsKey("eventcode");
    }

    static Stream<HashMap<String, String>> applyOperations(Stream<HashMap<String, String>> stream) {
        return stream
                .filter(MatchProcessor::containsValidHeaders);
    }


    static void fillPreparedStatement(PreparedStatement preparedStatement, HashMap<String, String> map) {
        try {
            preparedStatement.setString(1, map.get("team1name"));
            preparedStatement.setString(2, map.get("team2name"));
            preparedStatement.setString(3, map.get("locationid"));
            preparedStatement.setString(4, map.get("locationname"));
            preparedStatement.setString(5, map.get("matchid"));
            preparedStatement.setString(6, map.get("plannedtime"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
