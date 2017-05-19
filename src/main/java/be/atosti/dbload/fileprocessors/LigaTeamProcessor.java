package be.atosti.dbload.fileprocessors;

import be.atosti.dbload.PreparedStatementFiller;
import org.adrianwalker.multilinestring.Multiline;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;

/**
 * Created by thomas on 5/7/17.
 */
public class LigaTeamProcessor extends FileProcessor {


    /**
     INSERT INTO lf_group (tournament,`type`,event,devision,series) values ('2015','LIGA','MX',0,''),('2015','LIGA','M',0,''),('2015','LIGA','L',0,'');
     */
    @Multiline
    private static final String INSERT_FAKE_LIGA_GROUPS="";


    private LigaTeamProcessor(Path path) {
        this.path = path;
    }

    public static LigaTeamProcessor LigaTeamProcessorFactory(Path path) {
        return new LigaTeamProcessor(path);
    }
    public void load(Connection connection) throws SQLException {
        executeSQL(connection,INSERT_FAKE_LIGA_GROUPS);

        load(
                LigaTeamProcessor::applyOperations,
                Arrays.asList(
                        new PreparedStatementFiller(connection.prepareStatement(this.SQL), LigaTeamProcessor::fillPreparedStatement)
                ));
    }

    public static boolean containsValidHeaders(HashMap<String, String> r) {
        return r.containsKey("player_playerId")
                && r.containsKey("team_teamName")
                && r.containsKey("club_clubName");
    }


    static Stream<HashMap<String, String>> applyOperations(Stream<HashMap<String, String>> stream) {
        Map<String,Map<String,List<HashMap<String,String>>>> groupedResults = stream
                .filter(LigaTeamProcessor::containsValidHeaders)
                    .collect(
                            groupingBy(x -> x.get("team_teamName"),
                                groupingBy(x -> x.get("club_clubName"))));

        List<HashMap<String,String>> resultList = new ArrayList<>();
        groupedResults.forEach((teamName,g1) -> g1.forEach((clubName,g2)-> {
            HashMap<String,String> entry = new HashMap<>();
            entry.put("teamName",teamName);
            entry.put("clubName",clubName);
            entry.put("sequenceNumber",TeamProcessor.teamNameToSequenceNumber(teamName));
            entry.put("eventcode",GroupProcessor.teamNameToEventCode(teamName));
            resultList.add(entry);
        }));

        return resultList.stream();

    }

    /**
     INSERT lf_team (teamName,sequenceNumber,club_clubId, group_groupId,captainName) VALUES
     (?,
     ?,
     (select min(c.clubId) from lf_club c where c.clubName = ?),
     (select min(groupId) from lf_group where `type`='LIGA' and event=?),
     ''
     )
     */
    @Multiline
    private static final String SQL="";


    static void fillPreparedStatement(PreparedStatement preparedStatement, HashMap<String, String> map) {
        try {
            preparedStatement.setString(1, map.get("teamName"));
            preparedStatement.setString(2, map.get("sequenceNumber"));
            preparedStatement.setString(3, map.get("clubName"));
            preparedStatement.setString(4, map.get("eventcode"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }



}
