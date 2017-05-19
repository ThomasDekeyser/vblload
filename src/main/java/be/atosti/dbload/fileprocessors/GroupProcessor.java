package be.atosti.dbload.fileprocessors;

import be.atosti.dbload.PreparedStatementFiller;

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
public class GroupProcessor extends FileProcessor {

    private static final String SQL ="INSERT INTO lf_group (tournament,type,event,devision,series) values (?, ?, ?, ?, ?)";
    public static final String YEAR="2016";//TODO: externalize
    public static final String GROUP_TYPE="PROV";//TODO: externalize

    private GroupProcessor(Path path) {
        this.path = path;
    }

    public static GroupProcessor GroupProcessorFactory(Path path) {
        return new GroupProcessor(path);
    }
    public void load(Connection connection) throws SQLException {
        load(
                GroupProcessor::applyOperations,
                Arrays.asList(
                        new PreparedStatementFiller(connection.prepareStatement(this.SQL),GroupProcessor::fillPreparedStatement)
                ));
    }

    static boolean containsValidHeaders(HashMap<String, String> r) {
        return r.containsKey("clubcode")
                && r.containsKey("clubname")
                && r.containsKey("eventname");
    }


    static Stream<HashMap<String, String>> applyOperations(Stream<HashMap<String, String>> stream) {
        Map<String,Map<String,Map<String,List<HashMap<String,String>>>>> groupedResults = stream
                .filter(GroupProcessor::containsValidHeaders)
                .collect(
                groupingBy(x -> teamNameToEventCode(x.get("name")),
                        groupingBy(x -> drawNameToDevision(x.get("DrawName")),
                                groupingBy(x -> drawNameToSeries(x.get("DrawName"))))));

        List<HashMap<String,String>> resultList = new ArrayList<>();
        groupedResults.forEach((eventcode,g1) -> g1.forEach((devision,g2) -> g2.forEach((series,g3)-> {
            HashMap<String,String> entry = new HashMap<>();
            entry.put("tournament",YEAR);
            entry.put("type",GROUP_TYPE);
            entry.put("event",eventcode);
            entry.put("devision",devision);
            entry.put("series",series);

            resultList.add(entry);
        })));

        return resultList.stream();
    }

    public static String teamNameToEventCode(String teamName) {
        String result = "??";
        if (teamName.endsWith("G")) {
            result="MX";
        }else if (teamName.endsWith("H")) {
            result="M";
        }else if (teamName.endsWith("D")) {
            result = "L";
        }
        return result;
    }

    static String drawNameToDevision(String drawName) {
        return ""+drawName.charAt(0);
    }

    static String drawNameToSeries(String drawName) {
        String[] split =drawName.trim().split(" ");
        String lastElement = split[split.length-1];
        return lastElement.length()==1? lastElement : "";
    }


    static void fillPreparedStatement(PreparedStatement preparedStatement, HashMap<String, String> map) {
        try {
            preparedStatement.setString(1, map.get("tournament"));
            preparedStatement.setString(2, map.get("type"));
            preparedStatement.setString(3, map.get("event"));
            preparedStatement.setString(4, map.get("devision"));
            preparedStatement.setString(5, map.get("series"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
