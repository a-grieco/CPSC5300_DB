//
// Created by Kevin Lundeen on 4/6/17.
//

#include "SQLExec.h"

Tables * SQLExec::tables = nullptr;

std::ostream &operator<<(std::ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << std::endl << "+";
        for (u_long i = 0; i < qres.column_names->size(); i++)
            out << "------------+";
        out << std::endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << std::endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() { /* FIXME */ }

QueryResult *SQLExec::execute(const hsql::SQLStatement *statement) throw(SQLExecError) {
	
	if(SQLExec::tables == nullptr)
	{
		SQLExec::tables = new Tables();
	}

    try {
        switch (statement->type()) {
            case hsql::kStmtCreate:
                return create((const hsql::CreateStatement *) statement);
            case hsql::kStmtDrop:
                return drop((const hsql::DropStatement *) statement);
            case hsql::kStmtShow:
                return show((const hsql::ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError& e) {
        throw SQLExecError(std::string("DbRelationError: ") + e.what());
    }
}

QueryResult *SQLExec::create(const hsql::CreateStatement *statement) {
	
	

    return new QueryResult("execute create not implemented");
}

QueryResult *SQLExec::drop(const hsql::DropStatement *statement) {
    return new QueryResult("execute drop not implemented");
}

QueryResult *SQLExec::show(const hsql::ShowStatement *statement)  {
    /*return new QueryResult("execute show not implemented");*/

	switch(statement->type)
	{
	case hsql::ShowStatement::kTables: 
		return show_tables(statement);
		break;
	case hsql::ShowStatement::kColumns: 
		return show_columns(statement);
		break;
	case hsql::ShowStatement::kIndex: 
		return new QueryResult("statement type not found");
	default: ;
	}
}

QueryResult* SQLExec::show_tables(const hsql::ShowStatement* statement)
{
	// TODO: fix memeory leak QueryDestructor
	ValueDicts* rows = new ValueDicts;

	ColumnNames* column_names = new ColumnNames;
	ColumnAttributes* column_attributes = new ColumnAttributes;
	tables->get_columns(tables->TABLE_NAME, *column_names, *column_attributes);

	Handles* handles = tables->select();
	for(auto const& handle: *handles)
	{
		// TODO: skip first two rows
		ValueDict* row = tables->project(handle);
		(*rows).push_back(row);
	}
	
	return new QueryResult(column_names, column_attributes, rows, "show_tables complete");
}

QueryResult* SQLExec::show_columns(const hsql::ShowStatement* statement)
{
	Identifier _columns = Columns::TABLE_NAME;
	DbRelation& table = tables->get_table(_columns);

	std::string table_name = statement->tableName;

	Handles* handles = table.select();
	 

	return new QueryResult("poooop");
	//std::string message = "";

	//ColumnNames column_names;
	//ColumnAttributes column_attributes;
	//tables->get_columns(tables->TABLE_NAME, column_names, column_attributes);

	//for (Identifier name : column_names)
	//{
	//	message += name + "\n";
	//}
	//return new QueryResult(message);
}

void SQLExec::column_definition(const hsql::ColumnDefinition* col, Identifier& column_name, ColumnAttribute& column_attribute)
{
	column_name = col->name;
	//column_attribute = col->type;
}
