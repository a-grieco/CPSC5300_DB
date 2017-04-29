//============================================================================
// Name        : sql4300
// Author      : Kevin Lundeen
// Description : Relation manager project for CPSC4300/5300 Spring 2017
//============================================================================

#include <stdio.h>
#include <stdlib.h>
#include <cassert>
#include <cstring>
#include "db_cxx.h"
#include "SQLParser.h"
#include "ParseTreeToString.h"
#include "SQLExec.h"

const bool RUN_TEST = false;

void initialize_environment(char *envHome);

// test cases run when RUN_TEST is set to true
std::vector<std::string> test_cases = {
	"show tables", 
	"create table foo (id int, data text)",
	"show tables",
	"show columns from foo",
	"create index fx on foo (id)",
	"create index fz on foo (data)",
	"show index from foo",
	"insert into foo (id, data) values (1,\"one\")",
	"select * from foo",
	"insert into foo values (2, \"Two\"); insert into foo values (3, \"Three\"); insert into foo values (99, \"wowzers, Penny!!\")",
	"select * from foo",
	"select * from foo where id=3",
	"select * from foo where id=1 and data=\"one\"",
	"select * from foo where id=99 and data=\"nine\"",
	"select id from foo",
	"select data from foo where id=1",
	"delete from foo where id=1",
	"select * from foo",
	"delete from foo",
	"select * from foo",
	"insert into foo values (2, \"Two\"); insert into foo values (3, \"Three\"); insert into foo values (99, \"wowzers, Penny!!\")",
	"select * from foo",
	"drop index fz from foo",
	"show index from foo",
	"insert into foo (id) VALUES (100)",
	"select * from foo",
	"drop table foo",
	"show tables"
};

int main(int argc, char *argv[]) {

    if (argc != 2) {
        std::cerr << "Usage: cpsc4300: dbenvpath" << std::endl;
        return 1;
    }
    initialize_environment(argv[1]);

	// set RUN_TEST to true to run test cases
	// (will allow additional SQL commands when complete)
	int test_count = 0;

    while (true) {
        std::cout << "SQL> ";
		std::string query;
		if(RUN_TEST==true && test_count < test_cases.size())
		{
			query = test_cases.at(test_count);
			std::cout << query << std::endl;
			++test_count;
		} 
    	else
		{
			std::getline(std::cin, query);
		}        
        if (query.length() == 0)
            continue;
        if (query == "quit")
            break;
        if (query == "test") {
            std::cout << (test_heap_storage() ? "ok" : "failed") << std::endl;
            continue;
        }

        // parse and execute
        hsql::SQLParserResult *parse = hsql::SQLParser::parseSQLString(query);
        if (!parse->isValid()) {
            std::cout << "invalid SQL: " << query << std::endl;
            std::cout << parse->errorMsg() << std::endl;
        } else {
            for (uint i = 0; i < parse->size(); ++i) {
                const hsql::SQLStatement *statement = parse->getStatement(i);
                try {
                    std::cout << ParseTreeToString::statement(statement) << std::endl;
                    QueryResult *result = SQLExec::execute(statement);
                    std::cout << *result << std::endl;
                    delete result;
                } catch (SQLExecError& e) {
                    std::cout << std::string("Error: ") << e.what() << std::endl;
                }
            }
        }
        delete parse;
    }

    return EXIT_SUCCESS;
}

DbEnv *_DB_ENV;
void initialize_environment(char *envHome) {
    std::cout << "(sql4300: running with database environment at " << envHome
              << ")" << std::endl;

    DbEnv *env = new DbEnv(0U);
    env->set_message_stream(&std::cout);
    env->set_error_stream(&std::cerr);
    try {
        env->open(envHome, DB_CREATE | DB_INIT_MPOOL, 0);
    } catch (DbException &exc) {
        std::cerr << "(sql4300: " << exc.what() << ")";
        exit(1);
    }
    _DB_ENV = env;
    initialize_schema_tables();
}

