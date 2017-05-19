package be.atosti.dbload;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.function.BiConsumer;


public class PreparedStatementFiller {
    private PreparedStatement preparedStatement;
    private BiConsumer<PreparedStatement, HashMap<String, String>> preparedStatementHashMapBiConsumer;

    public PreparedStatementFiller(PreparedStatement preparedStatement,BiConsumer<PreparedStatement, HashMap<String, String>> preparedStatementHashMapBiConsumer) {
        this.preparedStatement = preparedStatement;
        this.preparedStatementHashMapBiConsumer = preparedStatementHashMapBiConsumer;
    }

    public PreparedStatement getPreparedStatement() {
        return preparedStatement;
    }

    public void setPreparedStatement(PreparedStatement preparedStatement) {
        this.preparedStatement = preparedStatement;
    }

    public BiConsumer<PreparedStatement, HashMap<String, String>> getPreparedStatementHashMapBiConsumer() {
        return preparedStatementHashMapBiConsumer;
    }

    public void setPreparedStatementHashMapBiConsumer(BiConsumer<PreparedStatement, HashMap<String, String>> preparedStatementHashMapBiConsumer) {
        this.preparedStatementHashMapBiConsumer = preparedStatementHashMapBiConsumer;
    }
}
