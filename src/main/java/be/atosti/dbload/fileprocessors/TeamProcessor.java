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
public class TeamProcessor extends FileProcessor {
    /**
     INSERT INTO lf_team (teamName,sequenceNumber,club_clubId, group_groupId, captainName,email) values
     (?, ?, ?, (select groupId from lf_group lfg where lfg.tournament = ? and lfg.event = ? and  lfg.devision = ? and lfg.series = ?), ?,?)
     */
    @Multiline
    private static final String SQL_INSERT_TEAM ="";

    /**
     update lf_club c set c.teamNamePrefix =
        (select substr(teamName,1,length(teamName)-INSTR(REVERSE(teamName),' ')) from lf_team t where t.club_clubId = c.clubId group by t.club_clubId)
     */
    @Multiline
    private static final String SQL_POST_OPERATION_SET_TEAMNAME_PREFIX="";

    private TeamProcessor(Path path) {
        this.path = path;
    }

    public static TeamProcessor TeamProcessorFactory(Path path) {
        return new TeamProcessor(path);
    }

    public void load(Connection connection) throws SQLException {
        load(
                TeamProcessor::applyOperations,
                Arrays.asList(
                        new PreparedStatementFiller(connection.prepareStatement(this.SQL_INSERT_TEAM),TeamProcessor::fillPreparedStatement)
                ));

        executeSQL(connection,SQL_POST_OPERATION_SET_TEAMNAME_PREFIX);
    }



    static boolean containsValidHeaders(HashMap<String, String> r) {
        return r.containsKey("clubcode")
                && r.containsKey("clubname")
                && r.containsKey("eventname");
    }


    static Stream<HashMap<String, String>> applyOperations(Stream<HashMap<String, String>> stream) {
        return stream
                .filter(TeamProcessor::containsValidHeaders)
                .map(m -> {
                    String teamName = m.get("name");
                    String drawName = m.get("DrawName");
                    String[] teamNameSplitted = teamName.split(" ");
                    String teamNameLastPart = teamNameSplitted[teamNameSplitted.length-1];

                    m.put("XXsequenceNumber",teamNameLastPart.substring(0,teamNameLastPart.length()-1));
                    m.put("XXevent",GroupProcessor.teamNameToEventCode(teamName));
                    m.put("XXdevision",GroupProcessor.drawNameToDevision(drawName));
                    m.put("XXseries",GroupProcessor.drawNameToSeries(drawName));

                    return m;
                });
    }


    static void fillPreparedStatement(PreparedStatement preparedStatement, HashMap<String, String> map) {
        try {
            preparedStatement.setString(1, map.get("name"));
            preparedStatement.setString(2, map.get("XXsequenceNumber"));
            preparedStatement.setString(3, map.get("clubcode"));
            preparedStatement.setString(4, GroupProcessor.YEAR);
            preparedStatement.setString(5, map.get("XXevent"));
            preparedStatement.setString(6, map.get("XXdevision"));
            preparedStatement.setString(7, map.get("XXseries"));
            preparedStatement.setString(8, map.get("contact"));//captain name
            preparedStatement.setString(9, map.get("email"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
