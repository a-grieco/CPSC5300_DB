//
// Created by Kevin Lundeen on 4/6/17.
//

#include "SQLExec.h"
#include <set>

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
	
	Identifier new_table_name = statement->tableName;
	std::vector<hsql::ColumnDefinition*>* new_table_columns = statement->columns;

	// insert new table into _tables table
	ValueDict new_row_in_columns;
	new_row_in_columns["table_name"] = Value(new_table_name);
	tables->insert(&new_row_in_columns);

	// insert new table columns into _columns (column by column)
	Identifier _columns = Columns::TABLE_NAME;
	DbRelation& table = tables->get_table(_columns);

	std::string column_name;
	hsql::ColumnDefinition::DataType column_type;
	std::string data_type;
	ValueDicts* new_table_rows = new ValueDicts;
	for(auto const &column_definition : *new_table_columns)
	{
		column_name = column_definition->name;
		column_type = column_definition->type;

		switch (column_type)
		{
		case hsql::ColumnDefinition::UNKNOWN:
			data_type = "UNKNOWN";
			break;
		case hsql::ColumnDefinition::TEXT:
			data_type = "TEXT";
			break;
		case hsql::ColumnDefinition::INT:
			data_type = "INT";
			break;
		case hsql::ColumnDefinition::DOUBLE:
			data_type = "DOUBLE";
			break;
		default:;
		}

		ValueDict row;
		row["table_name"] = new_table_name;
		row["column_name"] = column_name;	
		row["data_type"] = data_type;
		table.insert(&row);
	}

	// create new table
	DbRelation& db_relation = tables->get_table(new_table_name);
	db_relation.create();

	std::string message = "created " + new_table_name;
    return new QueryResult(message);
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
	default: 
		return new QueryResult("statement type not found");
	}
}

// returns true if the name is found and needs to be filtered
bool filtered(const std::string& name, const std::set<std::basic_string<char>>& set)
{
	std::set<std::basic_string<char>>::iterator pos = set.find(name);
	if(pos==set.end())
	{
		return false;	// not found, no need to be filtered
	}
	return true;	// name found, must be filtered
}

QueryResult* SQLExec::show_tables(const hsql::ShowStatement* statement)
{
	// TODO: fix memeory leak QueryDestructor
	ValueDicts* rows = new ValueDicts;

	ColumnNames* column_names = new ColumnNames;
	ColumnAttributes* column_attributes = new ColumnAttributes;
	tables->get_columns(tables->TABLE_NAME, *column_names, *column_attributes);

	Handles* handles = tables->select();

	std::set<std::string> filter {"_tables", "_columns"};

	int count = 0;	// tracks the number filtered

	for(auto const& handle: *handles)
	{
		// TODO: skip first two rows
		ValueDict* row = tables->project(handle);
		if(!filtered((row->at("table_name")).s, filter))
		{
			(*rows).push_back(row);
			count++;
		}		
	}

	std::string message = "successfully returned " +
		std::to_string(handles->size() - count) + " rows";
	
	return new QueryResult(column_names, column_attributes, rows, message);
}

QueryResult* SQLExec::show_columns(const hsql::ShowStatement* statement)
{
	Identifier _columns = Columns::TABLE_NAME;
	DbRelation& table = tables->get_table(_columns);

	std::string stmt_table_name = statement->tableName;
	Identifier _column_table_name = "table_name";
	Value input_table_name(stmt_table_name);
		
	ValueDict* results = new ValueDict;
	(*results)[_column_table_name] = input_table_name;

	Handles* handles = table.select(results);

	ValueDicts* rows = new ValueDicts;
	for (auto const& handle : *handles)
	{
		ValueDict* row = table.project(handle);
		(*rows).push_back(row);
	}

	ColumnNames* column_names = new ColumnNames;
	ColumnAttributes* column_attributes = new ColumnAttributes;
	tables->get_columns(_columns, *column_names, *column_attributes);

	std::string message = "successfully returned " + 
		std::to_string(handles->size()) + " rows";

	return new QueryResult(column_names, column_attributes, rows, message);
}