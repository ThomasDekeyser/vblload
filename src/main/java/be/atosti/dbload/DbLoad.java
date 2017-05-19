package be.atosti.dbload;

import be.atosti.dbload.fileprocessors.*;
import com.mysql.cj.jdbc.MysqlDataSource;

import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by thomas on 5/7/17.
 */
public class DbLoad {
    public static void main(String[] args) throws SQLException, FileNotFoundException {

        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUser("lionfreaky");
        dataSource.setPassword("lf-mariadb-pwd");
        dataSource.setServerName("localhost");
        dataSource.setPort(3406);
        dataSource.setDatabaseName("lionfreaky");

        Connection connection=null;
        try{
            connection = dataSource.getConnection();
            long startTime = System.currentTimeMillis();
            ClubProcessor.ClubProcessorFactory(Paths.get("src/main/resources/tmp", "clubs.csv")).load(connection);
            GroupProcessor.GroupProcessorFactory(Paths.get("src/main/resources/tmp", "teams.csv")).load(connection);
            TeamProcessor.TeamProcessorFactory(Paths.get("src/main/resources/tmp", "teams.csv")).load(connection);
            MatchProcessor.MatchProcessorFactory(Paths.get("src/main/resources/tmp", "matches.csv")).load(connection);
            PlayerProcessor.PlayerProcessorFactory(Paths.get("src/main/resources/tmp", "players.csv")).load(connection);
            long stopTime = System.currentTimeMillis();
            System.out.println("Load duration:"+(stopTime-startTime)+ "ms");

        } finally {
            connection.close();
        }
    }
}
