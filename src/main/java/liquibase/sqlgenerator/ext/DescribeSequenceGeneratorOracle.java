package liquibase.sqlgenerator.ext;

import liquibase.database.Database;
import liquibase.database.core.OracleDatabase;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.statement.ext.DescribeSequenceStatement;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Getting the current sequence value
 *
 * @author Leo Przybylski (leo [at] rsmart.com)
 */
public class DescribeSequenceGeneratorOracle extends AbstractSqlGenerator<DescribeSequenceStatement> {
    private static final String CREATE_SEQUENCE_STATEMENT = "CREATE TABLE IF NOT EXISTS %s (id bigint(19) NOT NULL auto_increment, PRIMARY KEY(id) )";
    private static final String SET_START_VALUE_STATEMENT = "INSERT INTO %s VALUES (%s)";

    @Override
    public int getPriority() {
        return 100;
    }
    
    @Override
    public boolean supports(final DescribeSequenceStatement statement, final Database database) {
        return database instanceof OracleDatabase;
    }

    @Override
    public ValidationErrors validate(CreateSequenceStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new ValidationErrors();
    }

    @Override
    public Sql[] generateSql(CreateSequenceStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        List<Sql> list = new ArrayList<Sql>();
        list.add(new UnparsedSql("select currval from " + statement.getSequenceName()));
        list.addAll(Arrays.asList(sqlGeneratorChain.generateSql(statement, database)));

        return list.toArray(new Sql[list.size()]);

    }
}
