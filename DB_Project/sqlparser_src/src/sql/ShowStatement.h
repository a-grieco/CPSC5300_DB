#ifndef __SHOW_STATEMENT_H__
#define __SHOW_STATEMENT_H__

#include "SQLStatement.h"
#include "../../../milestoneX/storage_engine.h"
#include <algorithm>

// Note: Implementations of constructors and destructors can be found in statements.cpp.
namespace hsql {
    // Represents SQL-extension Show statements.
    // Example "SHOW TABLES;"
    struct ShowStatement : SQLStatement {
        enum EntityType {
            kTables,
            kColumns,
            kIndex
        };

        ShowStatement(EntityType type);
        virtual ~ShowStatement();

	    ValueDict type[];
        char* tableName; // default: NULL
    };

} // namespace hsql
#endif