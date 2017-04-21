
#include "SQLExec.h"
#include "scopeguard.h"

Tables* SQLExec::tables = nullptr;
Indices* SQLExec::indices = nullptr;


std::ostream &operator<<(std::ostream &out, const QueryResult &qres) {
	if (qres.column_names != nullptr) {
		for (auto const &column_name : *qres.column_names)
			out << column_name << " ";
		out << std::endl << "+";
		for (int i = 0; i < qres.column_names->size(); i++)
			out << "----------+";
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
				case ColumnAttribute::BOOLEAN:
					out << (value.n == 0 ? "false" : "true");
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

QueryResult::~QueryResult() {
	if (column_names != nullptr)
		delete column_names;
	if (column_attributes != nullptr)
		delete column_attributes;
	if (rows != nullptr) {
		for (auto row : *rows)
			delete row;
		delete rows;
	}
}

QueryResult *SQLExec::execute(const hsql::SQLStatement *statement) throw(SQLExecError) {
	// initialize _tables table, if not yet present
	if (SQLExec::tables == nullptr)
	{
		SQLExec::tables = new Tables();
	}

	if (SQLExec::indices == nullptr)
	{
		SQLExec::indices = new Indices();
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

void SQLExec::column_definition(const hsql::ColumnDefinition *col, Identifier& column_name,
	ColumnAttribute& column_attribute) {
	column_name = col->name;
	switch (col->type) {
	case hsql::ColumnDefinition::INT:
		column_attribute.set_data_type(ColumnAttribute::INT);
		break;
	case hsql::ColumnDefinition::TEXT:
		column_attribute.set_data_type(ColumnAttribute::TEXT);
		break;
	case hsql::ColumnDefinition::DOUBLE:
	default:
		throw SQLExecError("unrecognized data type");
	}
}

QueryResult *SQLExec::create(const hsql::CreateStatement *statement) {
	switch (statement->type) {
	case hsql::CreateStatement::kTable:
		return create_table(statement);
	case hsql::CreateStatement::kIndex:
		return create_index(statement);
	default:
		return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
	}
}

QueryResult *SQLExec::create_table(const hsql::CreateStatement *statement) {
	Identifier table_name = statement->tableName;
	ColumnNames column_names;
	ColumnAttributes column_attributes;
	Identifier column_name;
	ColumnAttribute column_attribute;
	for (hsql::ColumnDefinition *col : *statement->columns) {
		column_definition(col, column_name, column_attribute);
		column_names.push_back(column_name);
		column_attributes.push_back(column_attribute);
	}

	// Add to schema: _tables and _columns
	ValueDict row;
	row["table_name"] = table_name;
	Handle t_handle = SQLExec::tables->insert(&row);  // Insert into _tables
	try {
		Handles c_handles;
		DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
		try {
			for (uint i = 0; i < column_names.size(); i++) {
				row["column_name"] = column_names[i];
				row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
				c_handles.push_back(columns.insert(&row));  // Insert into _columns
			}

			// Finally, actually create the relation
			DbRelation& table = SQLExec::tables->get_table(table_name);
			if (statement->ifNotExists)
				table.create_if_not_exists();
			else
				table.create();

		}
		catch (std::exception& e) {
			// attempt to remove from _columns
			try {
				for (auto const &handle : c_handles)
					columns.del(handle);
			}
			catch (...) {}
			throw;
		}

	}
	catch (std::exception& e) {
		try {
			// attempt to remove from _tables
			SQLExec::tables->del(t_handle);
		}
		catch (...) {}
		throw;
	}
	return new QueryResult("created " + table_name);
}

QueryResult *SQLExec::create_index(const hsql::CreateStatement *statement) {

	// Get the underlying table
	ColumnNames* column_names = new ColumnNames;
	ColumnAttributes* column_attributes = new ColumnAttributes;
	Tables::get_columns(statement->tableName, *column_names, *column_attributes);

	Handles* index_handles = new Handles;
	std::vector<char*>* index_columns = statement->indexColumns;

	ValueDict row;
	int seq_count = 0;

	row["table_name"] = statement->tableName;
	row["index_name"] = statement->indexName;
	row["index_type"] = statement->indexType;
	if (statement->indexType == "BTREE")
	{
		row["is_unique"] = true;
	}
	else if (statement->indexType == "HASH")
	{
		row["is_unique"] = false;
	}
	// Insert a row for each column in index key into _indices
	for (char* idx_col : *index_columns) {
		// Check that all the index columns exist in the table
		if (std::find(column_names->begin(), column_names->end(), idx_col)
			!= column_names->end()) {

			row["column_name"] = idx_col;
			row["seq_in_index"] = ++seq_count;

			Handle index_handle = indices->insert(&row);
			index_handles->push_back(index_handle);
		}
		// otherwise, throw an exception for nonexistant column
		else {
			std::string idx_col_name(idx_col);
			throw DbRelationError("Index " + idx_col_name +
				" does not exits in the table.");
		}
	}

	// delete from _indices if exception is thrown
	auto guard_insert_into_indices = scopeGuard([&]()
	{
		for (Handle idx_handle : *index_handles)
		{
			indices->del(idx_handle);
		}
	});

	DbIndex& index = indices->get_index(statement->tableName, statement->indexName);
	index.create();

	guard_insert_into_indices.dismiss();

	std::string idx_name = statement->indexName;
	return new QueryResult("created index " + idx_name);
}

// DROP ...
QueryResult *SQLExec::drop(const hsql::DropStatement *statement) {
	switch (statement->type) {
	case hsql::DropStatement::kTable:
		return drop_table(statement);
	case hsql::DropStatement::kIndex:
		return drop_index(statement);
	default:
		return new QueryResult("Only DROP TABLE and CREATE INDEX are implemented");
	}
}

QueryResult *SQLExec::drop_table(const hsql::DropStatement *statement) {
	Identifier table_name = statement->name;
	if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
		throw SQLExecError("cannot drop a schema table");

	ValueDict where;
	where["table_name"] = Value(table_name);

	// get the table
	DbRelation& table = SQLExec::tables->get_table(table_name);

	/* FIXME - drop any indices. */
	IndexNames index_names = indices->get_index_names(table_name);
	for(auto const index_name : index_names)
	{
		remove_rows(table_name, index_name);
	}

	// remove from _columns schema
	DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
	Handles* handles = columns.select(&where);
	for (auto const& handle : *handles)
		columns.del(handle);
	delete handles;

	// remove table
	table.drop();

	// finally, remove from _tables schema
	SQLExec::tables->del(*SQLExec::tables->select(&where)->begin()); // expect only one row from select

	return new QueryResult(std::string("dropped ") + table_name);
}

void SQLExec::remove_rows(Identifier name, Identifier index_name)
{
	ValueDict where;
	where["table_name"] = name;
	where["index_name"] = index_name;

	Handles* idx_handles = indices->select(&where);

	for (auto const& idx_handle : *idx_handles)
	{
		indices->del(idx_handle);
	}
	delete idx_handles;
}

QueryResult *SQLExec::drop_index(const hsql::DropStatement *statement) {
	// get a reference to the index and invoke the drop method on it
	DbIndex& index = 
		indices->get_index(statement->name, statement->indexName);
	index.drop();

	// remove all the rows from _indices for this index
	remove_rows(statement->name, statement->indexName);

	return new QueryResult("dropped index " + 
		std::string(statement->indexName));

	//ValueDict where;
	//where["table_name"] = statement->name;
	//where["index_name"] = statement->indexName;
	////DbRelation& indices = tables->get_table(Indices::TABLE_NAME);
	////indices.
	//Handles* idx_handles = indices->select();  //->select(&where);

	//for (auto const& idx_handle : *idx_handles)
	//{
	//	indices->del(idx_handle);
	//}
	//delete idx_handles;

	//return new QueryResult("dropped index " + 
	//	std::string(statement->indexName));
}



QueryResult *SQLExec::show(const hsql::ShowStatement *statement) {
	switch (statement->type) {
	case hsql::ShowStatement::kTables:
		return show_tables();
	case hsql::ShowStatement::kColumns:
		return show_columns(statement);
	case hsql::ShowStatement::kIndex:
		return show_index(statement);
	default:
		throw SQLExecError("unrecognized SHOW type");
	}
}

QueryResult *SQLExec::show_index(const hsql::ShowStatement *statement) {

	// Do a query on _indices for the given table name
	ValueDict where;
	where["table_name"] = Value(statement->tableName);
	Handles* idx_handles = indices->select(&where);

	ColumnNames* column_names = new ColumnNames;
	ColumnAttributes* column_attributes = new ColumnAttributes;
	tables->get_columns(Indices::TABLE_NAME, *column_names, *column_attributes);

	ValueDicts* rows = new ValueDicts;
	for (auto const handle : *idx_handles)
	{
		ValueDict* row = indices->project(handle);
		rows->push_back(row);
	}

	std::string message = "successfully returned " +
		std::to_string(idx_handles->size()) + " rows";

	return new QueryResult(column_names, column_attributes, rows, message);
}

QueryResult *SQLExec::show_tables() {
	ColumnNames* column_names = new ColumnNames;
	column_names->push_back("table_name");

	ColumnAttributes* column_attributes = new ColumnAttributes;
	column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

	Handles* handles = SQLExec::tables->select();
	//u_long n = handles->size() - 3;

	ValueDicts* rows = new ValueDicts;
	for (auto const& handle : *handles) {
		ValueDict* row = SQLExec::tables->project(handle, column_names);
		Identifier table_name = row->at("table_name").s;
		if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME
			&& table_name != Indices::TABLE_NAME)
			rows->push_back(row);
	}
	delete handles;
	return new QueryResult(column_names, column_attributes, rows,
		"successfully returned " + std::to_string(rows->size()) + " rows");
}

QueryResult *SQLExec::show_columns(const hsql::ShowStatement *statement) {
	DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

	ColumnNames* column_names = new ColumnNames;
	column_names->push_back("table_name");
	column_names->push_back("column_name");
	column_names->push_back("data_type");

	ColumnAttributes* column_attributes = new ColumnAttributes;
	column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

	ValueDict where;
	where["table_name"] = Value(statement->tableName);
	Handles* handles = columns.select(&where);
	u_long n = handles->size();

	ValueDicts* rows = new ValueDicts;
	for (auto const& handle : *handles) {
		ValueDict* row = columns.project(handle, column_names);
		rows->push_back(row);
	}
	delete handles;
	return new QueryResult(column_names, column_attributes, rows,
		"successfully returned " + std::to_string(n) + " rows");
}
