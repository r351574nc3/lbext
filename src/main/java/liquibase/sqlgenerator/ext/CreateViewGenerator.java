// Copyright 2011 Leo Przybylski. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification, are
// permitted provided that the following conditions are met:
//
//    1. Redistributions of source code must retain the above copyright notice, this list of
//       conditions and the following disclaimer.
//
//    2. Redistributions in binary form must reproduce the above copyright notice, this list
//       of conditions and the following disclaimer in the documentation and/or other materials
//       provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ''AS IS'' AND ANY EXPRESS OR IMPLIED
// WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
// ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
// ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// The views and conclusions contained in the software and documentation are those of the
// authors and should not be interpreted as representing official policies, either expressed
// or implied, of Leo Przybylski.
package liquibase.sqlgenerator.ext;

import liquibase.database.Database;
import liquibase.database.core.*;
import liquibase.exception.ValidationErrors;
import liquibase.logging.LogFactory;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.CreateViewStatement;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import static liquibase.ext.Constants.EXTENSION_PRIORITY;

/**
 *
 * @author Leo Przybylski (leo [at] rsmart.com)
 */
public class CreateViewGenerator extends liquibase.sqlgenerator.core.CreateViewGenerator {
    protected static List<SqlMappingHandler> replacements = Arrays.asList(
            new DecodeHandler(),
	    new TruncHandler()
									  );
    
    @Override
    public int getPriority() {
        return EXTENSION_PRIORITY;
    }

    public Sql[] generateSql(final CreateViewStatement statement, 
                             final Database database, 
                             final SqlGeneratorChain sqlGeneratorChain) {
	CreateViewStatement genStatement = statement;
        if (database instanceof liquibase.database.core.MySQLDatabase) {
	    genStatement = createMySqlSafeStatement(statement);
	}
	return super.generateSql(genStatement, database, sqlGeneratorChain);
    }

    public CreateViewStatement createMySqlSafeStatement(final CreateViewStatement statement) {
	final StringBuffer queryBuffer = new StringBuffer(statement.getSelectQuery());
	
	for (final SqlMappingHandler handler : replacements) {
	    while(handler.matches(queryBuffer)) {
		info("Got a match");
                handler.translate(queryBuffer);
	    }
	}
	
	return new CreateViewStatement(statement.getSchemaName(), 
				       statement.getViewName(), 
				       queryBuffer.toString(), 
				       statement.isReplaceIfExists());
    }

    protected void info(final String message) {
	LogFactory.getLogger().info(message);
    }

    protected void debug(final String message) {
	LogFactory.getLogger().debug(message);
    }
}
