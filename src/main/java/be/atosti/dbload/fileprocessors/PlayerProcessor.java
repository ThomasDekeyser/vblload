package be.atosti.dbload.fileprocessors;

import be.atosti.dbload.PreparedStatementFiller;
import org.adrianwalker.multilinestring.Multiline;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.minBy;

/**
 * Created by thomas on 5/7/17.
 */
public class PlayerProcessor extends FileProcessor {

    /**
     INSERT INTO lf_player (playerId,firstName,lastName,gender,club_clubId,type) VALUES
     (?,?,?,?,(select c.clubId from lf_club c where c.clubCode= ?),?)
     */
    @Multiline
    private static final String SQL_INSERT_PLAYERS ="";


    /**
     INSERT INTO lf_ranking (`date`,singles,doubles,mixed,player_playerId) VALUES
     (SYSDATE(),?,?,?,?)
     */
    @Multiline
    private static final String SQL_INSERT_RANKING ="";

    private  PlayerProcessor(Path path) {
        super.path = path;
    }

    public static PlayerProcessor PlayerProcessorFactory(Path path) {
        return new PlayerProcessor(path);
    }
    public void load(Connection connection) throws SQLException {

        load(
                PlayerProcessor::applyOperations,
                Arrays.asList(
                        new PreparedStatementFiller(connection.prepareStatement(this.SQL_INSERT_PLAYERS),PlayerProcessor::fillPreparedStatementToInsertPlayers),
                        new PreparedStatementFiller(connection.prepareStatement(this.SQL_INSERT_RANKING),PlayerProcessor::fillPreparedStatementToInsertRankings)
                ));
    }

    static boolean containsValidHeaders(HashMap<String, String> r) {
        return r.containsKey("groupcode")
                && r.containsKey("groupname")
                && r.containsKey("code")
                && r.containsKey("memberid");
    }



    static Stream<HashMap<String, String>> applyOperations(Stream<HashMap<String, String>> stream) {
        Map<String,HashMap<String, String>> f= stream.filter(PlayerProcessor::containsValidHeaders)
               .map(m -> {

                    m.put("XXrolePriority",givePriorityBasedOnRole(m.get("role")));
                    m.put("XXgender","V".equalsIgnoreCase(m.get("gender"))? "F":m.get("gender"));
                    m.put("XXtype",giveTypeCodeBasedOnType(m.get("TypeName")));

                    return m;
               })
               .collect(groupingBy(m -> m.get("memberid"),
                       collectingAndThen(
                               minBy(Comparator.comparing(x -> x.get("XXrolePriority"))),
                                Optional::get
                                )));

        return f.values().stream();

    }

    static String givePriorityBasedOnRole(String role) {
        String rolePriority="4";
        if ("Uitgeleende speler".equals(role)) {
            rolePriority="1";
        } else if (role.startsWith("Speler")) {
            rolePriority="2";
        } else if (role.startsWith("KYU")) {
            rolePriority="3";
        }
        return rolePriority;
    }

    static String giveTypeCodeBasedOnType(String typeName)  {
        String xxType = "";
        if(typeName.startsWith("Recreant")) {
            xxType="R";
        } else if (typeName.startsWith("Competitie")) {
            xxType="C";
        } else if (typeName.startsWith("Jeugd")) {
            xxType="J";
        }
        return xxType;
    }



    static void fillPreparedStatementToInsertPlayers(PreparedStatement preparedStatement, HashMap<String, String> map) {
        try {
            preparedStatement.setString(1, map.get("memberid"));
            preparedStatement.setString(2, map.get("firstname"));
            preparedStatement.setString(3, map.get("lastname"));
            preparedStatement.setString(4, map.get("XXgender"));
            preparedStatement.setString(5, map.get("groupcode"));
            preparedStatement.setString(6, map.get("XXtype"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    static void fillPreparedStatementToInsertRankings(PreparedStatement preparedStatement, HashMap<String, String> map) {
        try {
            preparedStatement.setString(1, map.get("PlayerLevelSingle"));
            preparedStatement.setString(2, map.get("PlayerLevelDouble"));
            preparedStatement.setString(3, map.get("PlayerLevelMixed"));
            preparedStatement.setString(4, map.get("memberid"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
