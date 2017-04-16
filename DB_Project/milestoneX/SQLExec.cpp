//
// Adrienne Grieco
// 4/16/2017
//

#include "SQLExec.h"
#include <set>
#include "scopeguard.h"

Tables * SQLExec::tables = nullptr;

std::ostream &operator<<(std::ostream &out, const QueryResult &qres) {
	if (qres.column_names != nullptr) {
		for (auto const &column_name : *qres.column_names)
			out << column_name << " ";
		out << std::endl << "+";
		for (u_long i = 0; i < qres.column_names->size(); i++)
			out << "------------+";
		out << std::endl;
		for (auto const &row : *qres.rows) {
			for (auto const &column_name : *qres.column_names) {
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

	if (SQLExec::tables == nullptr)
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
	}
	catch (DbRelationError& e) {
		throw SQLExecError(std::string("DbRelationError: ") + e.what());
	}
}

QueryResult *SQLExec::create(const hsql::CreateStatement *statement) {
	if (statement->type != hsql::CreateStatement::kTable)
	{
		throw SQLExecError("unrecognized CREATE type");
	}

	Identifier new_table_name = statement->tableName;
	std::vector<hsql::ColumnDefinition*>* new_table_columns =
		statement->columns;

	// insert new table into _tables
	ValueDict new_table;
	new_table["table_name"] = Value(new_table_name);
	Handle new_table_del = tables->insert(&new_table);

	// delete table from _tables if exception is thrown
	auto guard_insert_into_tables = scopeGuard([&]() {
		tables->del(new_table_del);
	});

	// insert new table's columns into _columns
	DbRelation& columns_relation = tables->get_table(Columns::TABLE_NAME);

	std::string column_name;
	hsql::ColumnDefinition::DataType column_type;
	std::string data_type;

	Handles new_table_columns_del;

	for (auto const &new_table_column : *new_table_columns)
	{
		column_name = new_table_column->name;
		column_type = new_table_column->type;

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
		Handle new_table_column_del = columns_relation.insert(&row);
		new_table_columns_del.push_back(new_table_column_del);
	}

	// delete rows added to _columns if exception thrown
	auto guard_insert_into_columns = scopeGuard([&]() {
		for (auto const column_del : new_table_columns_del)
		{
			columns_relation.del(column_del);
		}
	});

	// create new table
	DbRelation& new_table_relation = tables->get_table(new_table_name);
	new_table_relation.create();

	// delete new table if exception thrown
	auto guard_add_table = scopeGuard([&]() {
		new_table_relation.drop();
	});

	guard_insert_into_tables.dismiss();
	guard_insert_into_columns.dismiss();
	guard_add_table.dismiss();

	return new QueryResult("created " + new_table_name);
}

QueryResult *SQLExec::drop(const hsql::DropStatement *statement) {
	if (statement->type != hsql::DropStatement::kTable)
	{
		throw SQLExecError("unrecognized DROP type");
	}

	Identifier table_name = statement->name;
	ValueDict table_to_drop;
	table_to_drop["table_name"] = Value(table_name);

	Handles* drop_handles = tables->select(&table_to_drop);
	if (drop_handles->size() > 0)
	{
		DbRelation& columns_relation = tables->get_table(Columns::TABLE_NAME);

		Handles* col_rows_to_delete = columns_relation.select(&table_to_drop);

		for (auto const col_row : (*col_rows_to_delete))
		{
			columns_relation.del(col_row);
		}

		try
		{
			DbRelation& drop_table_relation = tables->get_table(table_name);
			drop_table_relation.drop();
		}
		catch (...) {}

		// delete table from _tables
		Handle drop_handle = drop_handles->at(0);
		tables->del(drop_handle);

		return new QueryResult("dropped " + table_name);
	}

	return new QueryResult("unable to drop " + table_name +
		", table does not exist");
}

QueryResult *SQLExec::show(const hsql::ShowStatement *statement) {

	switch (statement->type)
	{
	case hsql::ShowStatement::kTables:
		return show_tables(statement);
	case hsql::ShowStatement::kColumns:
		return show_columns(statement);
	case hsql::ShowStatement::kIndex:
	default:
		return new QueryResult("statement type not found");
	}
}

// returns true if the given name exists in the set and needs to be filtered
// (i.e. if name is "_tables" and set has "_tables" & "_columns", return true)
bool filtered(const std::string& name,
	const std::set<std::basic_string<char>>& set)
{
	std::set<std::basic_string<char>>::iterator it = set.find(name);
	if (it == set.end())
	{
		return false;	// name not found, no need to filter
	}
	return true;	// name found, filter
}

QueryResult* SQLExec::show_tables(const hsql::ShowStatement* statement)
{
	// TODO: fix memeory leak QueryDestructor
	ValueDicts* rows = new ValueDicts;

	ColumnNames* column_names = new ColumnNames;
	ColumnAttributes* column_attributes = new ColumnAttributes;
	tables->get_columns(tables->TABLE_NAME, *column_names, *column_attributes);

	Handles* handles = tables->select();

	std::set<std::string> filter_set{ "_tables", "_columns" };

	int count = 0;	// track number of rows filtered

	for (auto const& handle : *handles)
	{
		ValueDict* row = tables->project(handle);
		if (!filtered((row->at("table_name")).s, filter_set))
		{
			(*rows).push_back(row);

		}
		else
		{
			count++;
		}
	}

	std::string message = "successfully returned " +
		std::to_string(handles->size() - count) + " rows";

	return new QueryResult(column_names, column_attributes, rows, message);
}

QueryResult* SQLExec::show_columns(const hsql::ShowStatement* statement)
{
	DbRelation& table = tables->get_table(Columns::TABLE_NAME);

	std::string table_name = statement->tableName;
	Value input_table_name(table_name);

	ValueDict* results = new ValueDict;
	(*results)["table_name"] = input_table_name;

	Handles* handles = table.select(results);

	ValueDicts* rows = new ValueDicts;
	for (auto const& handle : *handles)
	{
		ValueDict* row = table.project(handle);
		(*rows).push_back(row);
	}

	ColumnNames* column_names = new ColumnNames;
	ColumnAttributes* column_attributes = new ColumnAttributes;
	tables->get_columns(Columns::TABLE_NAME, *column_names, *column_attributes);

	std::string message = "successfully returned " +
		std::to_string(handles->size()) + " rows";

	return new QueryResult(column_names, column_attributes, rows, message);
}