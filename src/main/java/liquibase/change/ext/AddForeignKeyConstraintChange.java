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
package liquibase.change.ext;

import liquibase.change.AbstractChange;
import liquibase.change.Change;
import liquibase.change.ChangeMetaData;
import liquibase.database.Database;
import liquibase.database.structure.ForeignKeyConstraintType;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.AddForeignKeyConstraintStatement;

import static liquibase.ext.Constants.EXTENSION_PRIORITY;

/**
 * Adds a foreign key constraint to an existing column. Making resulting foreign keys ignore schema information
 *
 * Leo Przybylski (leo [at] rsmart.com) 
 */
public class AddForeignKeyConstraintChange extends liquibase.change.core.AddForeignKeyConstraintChange {
    public AddForeignKeyConstraintChange() {
        setPriority(EXTENSION_PRIORITY);
    }


    public SqlStatement[] generateStatements(Database database) {

        boolean deferrable = false;
        if (getDeferrable() != null) {
            deferrable = getDeferrable();
        }

        boolean initiallyDeferred = false;
        if (getInitiallyDeferred() != null) {
            initiallyDeferred = getInitiallyDeferred();
        }

        return new SqlStatement[]{
                new AddForeignKeyConstraintStatement(getConstraintName(), null,
                                                     getBaseTableName(),
                                                     getBaseColumnNames(),
                                                     null,
                                                     getReferencedTableName(),
                                                     getReferencedColumnNames())
                .setDeferrable(deferrable)
                .setInitiallyDeferred(initiallyDeferred)
                .setOnUpdate(getOnUpdate())
                .setOnDelete(getOnDelete())
                .setReferencesUniqueColumn(getReferencesUniqueColumn())
        };
    }
}
