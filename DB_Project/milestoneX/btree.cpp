
#include "btree.h"
#include <string>
#include <iterator>
#include "SQLExec.h"
#include "ParseTreeToString.h"


/************
 * BTreeBase
 ************/

BTreeBase::BTreeBase(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique)
	: DbIndex(relation, name, key_columns, unique),
	stat(nullptr),
	root(nullptr),
	closed(true),
	file(relation.get_table_name() + "-" + name),
	key_profile() {
	if (!unique)
		throw DbRelationError("BTree index must have unique key");
	build_key_profile();
}

BTreeBase::~BTreeBase() {
	delete this->stat;
	delete this->root;
}

// Create the index.
void BTreeBase::create() {
	this->file.create();
	this->stat = new BTreeStat(this->file, STAT, STAT + 1, this->key_profile);
	this->root = make_leaf(this->stat->get_root_id(), true);
	this->closed = false;

	Handles *handles = nullptr;
	try {
		// now build the index! -- add every row from relation into index
		//this->file.begin_write();
		handles = this->relation.select();
		for (auto const &handle : *handles)
			insert(handle);
		//this->file.end_write();
		delete handles;
	}
	catch (...) {
		delete handles;
		drop();
		throw;
	}
}

// Drop the index.
void BTreeBase::drop() {
	this->file.drop();
	this->closed = true;
}

// Open existing index. Enables: lookup, range, insert, delete, update.
void BTreeBase::open() {
	if (this->closed) {
		this->file.open();
		this->stat = new BTreeStat(this->file, STAT, this->key_profile);
		if (this->stat->get_height() == 1)
			this->root = make_leaf(this->stat->get_root_id(), false);
		else
			this->root = new BTreeInterior(this->file, this->stat->get_root_id(), this->key_profile, false);
		this->closed = false;
	}
}

// Closes the index. Disables: lookup, range, insert, delete, update.
void BTreeBase::close() {
	this->file.close();
	delete this->stat;
	this->stat = nullptr;
	delete this->root;
	this->root = nullptr;
	this->closed = true;
}

// Find all the rows whose columns are equal to key. Assumes key is a dictionary whose keys are the column
// names in the index. Returns a list of row handles.
Handles* BTreeBase::lookup(ValueDict* key_dict) {
	open();
	KeyValue *key = tkey(key_dict);
	BTreeLeafBase *leaf = _lookup(this->root, this->stat->get_height(), key);
	Handles *handles = new Handles();
	try {
		BTreeLeafValue value = leaf->find_eq(key);
		handles->push_back(value.h);
	}
	catch (std::out_of_range &e) {
		; // not found, so we return an empty list
	}
	delete key;
	return handles;
}

// Recursive lookup.
BTreeLeafBase* BTreeBase::_lookup(BTreeNode *node, uint depth, const KeyValue* key) {
	if (depth == 1) { // base case: leaf
		return (BTreeLeafBase *)node;
	}
	else { // interior node: find the block to go to in the next level down and recurse there
		BTreeInterior *interior = (BTreeInterior *)node;
		return _lookup(find(interior, depth, key), depth - 1, key);
	}
}

Handles* BTreeBase::_range(KeyValue *tmin, KeyValue *tmax, bool return_keys) {
	Handles *results = new Handles();
	BTreeLeafBase *start = _lookup(this->root, this->stat->get_height(), tmin);
	for (auto const& mval : start->get_key_map()) {
		if (tmax != nullptr && mval.first > *tmax)
			return results;
		if (tmin == nullptr || mval.first >= *tmin) {
			if (return_keys)
				results->push_back(Handle(mval.first));
			else
				results->push_back(Handle(mval.second.h));
		}
	}
	BlockID next_leaf_id = start->get_next_leaf();
	while (next_leaf_id > 0) {
		BTreeLeafBase *next_leaf = this->make_leaf(next_leaf_id, false);
		for (auto const& mval : start->get_key_map()) {
			if (tmax != nullptr && mval.first > *tmax)
				return results;
			if (return_keys)
				results->push_back(Handle(mval.first));
			else
				results->push_back(Handle(mval.second.h));
		}
		next_leaf_id = next_leaf->get_next_leaf();
	}
	return results;
}

// Insert a row with the given handle. Row must exist in relation already.
void BTreeBase::insert(Handle handle) {
	ValueDict *row = this->relation.project(handle, &this->key_columns);
	KeyValue *key = tkey(row);
	delete row;

	Insertion split = _insert(this->root, this->stat->get_height(), key, handle);
	if (!BTreeNode::insertion_is_none(split))
		split_root(split);
}

// if we split the root grow the tree up one level
void BTreeBase::split_root(Insertion insertion) {
	BlockID rroot = insertion.first;
	KeyValue boundary = insertion.second;
	BTreeInterior *root = new BTreeInterior(this->file, 0, this->key_profile, true);
	root->set_first(this->root->get_id());
	root->insert(&boundary, rroot);
	root->save();
	this->stat->set_root_id(root->get_id());
	this->stat->set_height(this->stat->get_height() + 1);
	this->stat->save();
	this->root = root;
}

// Recursive insert. If a split happens at this level, return the (new node, boundary) of the split.
Insertion BTreeBase::_insert(BTreeNode *node, uint depth, const KeyValue* key, BTreeLeafValue leaf_value) {
	if (depth == 1) {
		BTreeLeafBase *leaf = (BTreeLeafBase *)node;
		try {
			return leaf->insert(key, leaf_value);
		}
		catch (DbBlockNoRoomError &e) {
			return leaf->split(make_leaf(0, true), key, leaf_value);
		}
	}
	else {
		BTreeInterior *interior = (BTreeInterior *)node;
		Insertion new_kid = _insert(find(interior, depth, key), depth - 1, key, leaf_value);
		if (!BTreeNode::insertion_is_none(new_kid)) {
			BlockID nnode = new_kid.first;
			KeyValue boundary = new_kid.second;
			return interior->insert(&boundary, nnode);
		}
		return BTreeNode::insertion_none();
	}
}

// Call the interior node's find method and construct an appropriate BTreeNode at the next level with the response
BTreeNode *BTreeBase::find(BTreeInterior *node, uint height, const KeyValue* key) {
	BlockID down = node->find(key);
	if (height == 2)
		return make_leaf(down, false);
	else
		return new BTreeInterior(this->file, down, this->key_profile, false);
}

// Delete an index entry
void BTreeBase::del(Handle handle)
{
	KeyValue* d_tkey = tkey(relation.project(handle));
	BTreeLeafBase* leaf = _lookup(root, stat->get_height(), d_tkey);

	LeafMap leaf_keys = leaf->get_key_map();
	if (leaf_keys.find(*d_tkey) == leaf_keys.end())
	{
		throw DbRelationError("key to be deleted not found in index");
	}

	leaf_keys.erase(*d_tkey);
	leaf->save();
}

// Figure out the data types of each key component and encode them in self.key_profile
void BTreeBase::build_key_profile() {
	ColumnAttributes *key_attributes = this->relation.get_column_attributes(this->key_columns);
	for (auto& ca : *key_attributes)
		key_profile.push_back(ca.get_data_type());
	delete key_attributes;
}

KeyValue *BTreeBase::tkey(const ValueDict *key) const {
	try
	{
		KeyValue *kv = new KeyValue();
		for (auto& col_name : this->key_columns)
			kv->push_back(key->at(col_name));
		return kv;
	}
	catch (...)
	{
		return nullptr;
	}
}


/************
 * BTreeIndex
 ************/

BTreeIndex::BTreeIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique)
	: BTreeBase(relation, name, key_columns, unique) {
}

BTreeIndex::~BTreeIndex() {
}

// Construct an appropriate leaf
BTreeLeafBase *BTreeIndex::make_leaf(BlockID id, bool create) {
	return new BTreeLeafIndex(this->file, id, this->key_profile, create);
}

// Range of values in index
Handles* BTreeIndex::range(ValueDict* min_key, ValueDict* max_key) {
	KeyValue *tmin = tkey(min_key);
	KeyValue *tmax = tkey(max_key);
	Handles *handles = _range(tmin, tmax, false);
	delete tmin;
	delete tmax;
	return handles;
}


/************
 * BTreeFile
 ************/

BTreeFile::BTreeFile(DbRelation& relation,
	Identifier name,
	ColumnNames key_columns,
	ColumnNames non_key_column_names,
	ColumnAttributes non_key_column_attributes,
	bool unique)
	: BTreeBase(relation, name, key_columns, unique),
	non_key_column_names(non_key_column_names),
	non_key_column_attributes(non_key_column_attributes) {
}

BTreeFile::~BTreeFile() {
}

// Construct an appropriate leaf
BTreeLeafBase *BTreeFile::make_leaf(BlockID id, bool create) {
	return new BTreeLeafFile(this->file, id, this->key_profile,
		this->non_key_column_names, this->non_key_column_attributes, create);
}

// Range of values in file
Handles* BTreeFile::range(KeyValue *tmin, KeyValue *tmax) {
	return _range(tmin, tmax, true);
}


// Get the values not in the primary key (Throws std::out_of_range if not found.)
ValueDict* BTreeFile::lookup_value(ValueDict* key_dict) {
	open();
	KeyValue *key = tkey(key_dict);
	BTreeLeafBase *leaf = _lookup(this->root, this->stat->get_height(), key);
	BTreeLeafValue value = leaf->find_eq(key);
	return value.vd;
	delete key;
}

// Insert a row with the given handle. Row must exist in relation already.
void BTreeFile::insert_value(ValueDict *row) {
	KeyValue *key = tkey(row);
	BTreeLeafValue value(row);
	Insertion split = _insert(this->root, this->stat->get_height(), key, value);
	if (!BTreeNode::insertion_is_none(split))
		split_root(split);
}



/************
 * BTreeTable
 ************/

BTreeTable::BTreeTable(Identifier table_name, ColumnNames column_names,
	ColumnAttributes column_attributes, const ColumnNames& primary_key)
	: DbRelation(table_name, column_names, column_attributes, primary_key)
{
	ColumnNames non_key_column_names;
	ColumnAttributes non_key_column_attributes;

	int count = 0;
	for (auto column_name : column_names)
	{
		if (std::find(primary_key.begin(), primary_key.end(), column_name)
			== primary_key.end())
		{
			non_key_column_names.push_back(column_name);
			non_key_column_attributes.push_back(column_attributes.at(count));
		}
		count++;
	}
	index = new BTreeFile(*this, table_name, primary_key, non_key_column_names,
		non_key_column_attributes, true);
}

void BTreeTable::create()
{
	index->create();
}

void BTreeTable::create_if_not_exists()
{
	try
	{
		open();
	}
	catch (DbRelationError)
	{
		create();
	}
}

void BTreeTable::drop()
{
	index->drop();
}

void BTreeTable::open()
{
	index->open();
}

void BTreeTable::close()
{
	index->close();
}

Handle BTreeTable::insert(const ValueDict* row)
{
	ValueDict* _row = validate(row);
	index->insert_value(_row);

	return Handle(*(index->tkey(_row)));
}

void BTreeTable::update(const Handle handle, const ValueDict* new_values)
{
	ValueDict* row = project(handle);

	// create a copy of row (new_row)
	ValueDict* new_row = new ValueDict;
	*new_row = *row;

	for (auto key : *new_values)
	{
		new_row->at(key.first) = new_values->at(key.first);
	}

	new_row = validate(new_row);
	KeyValue* new_tkey = index->tkey(new_row);

	index->del(handle);
	index->insert_value(new_row);
}

void BTreeTable::del(const Handle handle)
{
	index->del(handle);
}

Handles* BTreeTable::select()
{
	return select(nullptr);
}

Handles* BTreeTable::select(const ValueDict* where)
{
	Handles* ret_handles = new Handles;

	KeyValue* min_value = new KeyValue;
	KeyValue* max_value = new KeyValue;
	ValueDict* additional_where = new ValueDict;

	make_range(where, min_value, max_value, additional_where);

	Handles* tkeys = index->range(min_value, max_value);
	for (auto tkey : *tkeys)
	{
		if (additional_where == nullptr || selected(tkey, additional_where))
		{
			ret_handles->push_back(tkey);
		}
	}
	return ret_handles;
}

Handles* BTreeTable::select(Handles *current_selection, const ValueDict* where)
{
	Handles* ret_handles = new Handles;

	KeyValue* min_value = new KeyValue;
	KeyValue* max_value = new KeyValue;
	ValueDict* additional_where = new ValueDict;

	make_range(where, min_value, max_value, additional_where);

	for (auto tkey : *current_selection)
	{
		if (additional_where == nullptr || selected(tkey, additional_where))
		{
			ret_handles->push_back(tkey);
		}
	}
	return ret_handles;
}

ValueDict* BTreeTable::getValueDict(Handle handle)
{
	int count = 0;
	ValueDict* p_tkey = new ValueDict;

	for (auto c_name : *primary_key)
	{
		(*p_tkey)[c_name] = handle.key_value[count];
		++count;
	}
	return p_tkey;
}

ValueDict* BTreeTable::project(Handle handle)
{
	ValueDict* pk_dictionary = getValueDict(handle);
	ValueDict* p_row_no_pk = index->lookup_value(pk_dictionary);	// everyting but pk

	if (p_row_no_pk->empty())
	{
		throw DbRelationError("Cannot project: invalid handle");
	}

	// add pks to this
	for (auto thing : *pk_dictionary)
	{
		(*p_row_no_pk)[thing.first] = thing.second;
	}

	return p_row_no_pk;
}

ValueDict* BTreeTable::project(Handle handle, const ColumnNames* column_names)
{
	ValueDict* full_row = project(handle);
	ValueDict* result_row = new ValueDict;

	for (auto c_name : *column_names)
	{
		if (full_row->find(c_name) != full_row->end())
		{
			(*result_row)[c_name] = (*full_row)[c_name];
		}
	}
	return result_row;
}

ValueDict* BTreeTable::validate(const ValueDict* row) const
{
	ValueDict* full_row = new ValueDict;

	for (Identifier column_name : column_names)
	{
		if (row->find(column_name) == row->end())
		{
			throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
		}
		else
		{
			Value v = row->at(column_name);
			(*full_row)[column_name] = v;
		}
	}

	return full_row;
}

// checks if given record succeeds given where clause
bool BTreeTable::selected(Handle handle, const ValueDict* where)
{
	// iterate through all the column names instead?
	ValueDict* s_row = project(handle, where);
	for (auto w : *where)
	{
		if ((*s_row)[w.first] != w.second)
		{
			return false;
		}
	}
	return true;
}

void BTreeTable::make_range(const ValueDict *where,
	KeyValue *&minval, KeyValue *&maxval, ValueDict *&additional_where)
{
	if (where == nullptr)
	{
		minval = nullptr;
		maxval = nullptr;
		additional_where = nullptr;
	}
	else
	{
		additional_where->clear();
		additional_where->insert(where->begin(), where->end());


		//std::insert_iterator<ValueDict> ins_iter(*additional_where);
		//std::copy(where->begin(), where->end(), bii);
		//*additional_where = ValueDict(*where);
		KeyValue* tkey = new KeyValue;

		for (auto c : *primary_key)
		{

			if (additional_where->find(c) != additional_where->end())
			{
				additional_where->erase(c);
			}

			if (where->find(c) != where->end())
			{
				tkey->push_back(where->at(c));
			}
		}
		if (additional_where->empty())
		{
			delete additional_where;
			additional_where = nullptr;
		}

		if (tkey->size() > 0)
		{
			minval = tkey;
			maxval = tkey;
		}
		else
		{
			minval = nullptr;
			maxval = nullptr;
		}
	}
}


bool test_btree() {
	ColumnNames column_names;
	column_names.push_back("a");
	column_names.push_back("b");
	ColumnAttributes column_attributes;
	column_attributes.push_back(ColumnAttribute(ColumnAttribute::INT));
	column_attributes.push_back(ColumnAttribute(ColumnAttribute::INT));
	HeapTable table("__test_btree", column_names, column_attributes);
	table.create();
	ValueDict row1, row2;
	row1["a"] = Value(12);
	row1["b"] = Value(99);
	row2["a"] = Value(88);
	row2["b"] = Value(101);
	table.insert(&row1);
	table.insert(&row2);
	for (int i = 0; i < 1000; i++) {
		ValueDict row;
		row["a"] = Value(i + 100);
		row["b"] = Value(-i);
		table.insert(&row);
	}
	column_names.clear();
	column_names.push_back("a");
	BTreeIndex index(table, "fooindex", column_names, true);
	index.create();
	ValueDict lookup;
	lookup["a"] = 12;
	Handles *handles = index.lookup(&lookup);
	ValueDict *result = table.project(handles->back());
	if (*result != row1) {
		std::cout << "first lookup failed" << std::endl;
		return false;
	}
	delete handles;
	delete result;
	lookup["a"] = 88;
	handles = index.lookup(&lookup);
	result = table.project(handles->back());
	if (*result != row2) {
		std::cout << "second lookup failed" << std::endl;
		return false;
	}
	delete handles;
	delete result;
	lookup["a"] = 6;
	handles = index.lookup(&lookup);
	if (handles->size() != 0) {
		std::cout << "third lookup failed" << std::endl;
		return false;
	}
	delete handles;

	for (uint j = 0; j < 10; j++)
		for (int i = 0; i < 1000; i++) {
			lookup["a"] = i + 100;
			handles = index.lookup(&lookup);
			result = table.project(handles->back());
			row1["a"] = i + 100;
			row1["b"] = -i;
			if (*result != row1) {
				std::cout << "lookup failed " << i << std::endl;
				return false;
			}
			delete handles;
			delete result;
		}
	index.drop();
	table.drop();
	return true;
}

void run_test_statement(std::string sql) {

	hsql::SQLParserResult *parse = hsql::SQLParser::parseSQLString(sql);

	auto statement = parse->getStatement(0);
	std::cout << ParseTreeToString::statement(statement) << std::endl;
	try
	{
		QueryResult *result = SQLExec::execute(statement);
		std::cout << *result << std::endl;

	}
	catch (std::exception& e)
	{
		std::cout << "Exception running the test statement: " << e.what() << std::endl;
	}
}

std::ostream& operator<<(std::ostream& strm, const Value& val){
	switch (val.data_type)
	{
	case ColumnAttribute::INT: return strm << val.n;
	case ColumnAttribute::TEXT: return strm << val.s;
	case ColumnAttribute::BOOLEAN: return strm << "don't know how to print bools";
	default: return strm << "huh???";
	}
	return strm;
}

std::ostream& operator<<(std::ostream& strm, const Handle& h){
	
	for (const Value& val : h.key_value)
	{
		strm << val << " ";
	}
	return strm;
}

bool test_table()
{
	//ColumnNames column_names = { "id", "a", "b" };
	//ColumnAttributes column_attributes = {
	//	ColumnAttribute(ColumnAttribute::INT),
	//	ColumnAttribute(ColumnAttribute::INT),
	//	ColumnAttribute(ColumnAttribute::TEXT)
	//};
	//ColumnNames primary_key = { "id" };

	//BTreeTable table = BTreeTable("_test_btable", column_names, column_attributes, primary_key);
	////table.create_if_not_exists();	
	//								/* TODO: create_f_not_exists() & table.open() are broken
	//								 * for the same reason, what is that reason?
	//								 * Error thrown in HeapFile::db_open(uint flags)
	//								 * (see heap_storage.cpp line 186 HeapFile::create(void){ bd_open(DB_CREATE|DB_EXCL); }
	//								 */
	//table.create();
	//table.close();
	//table.open();


	// for now use statement to create the table so that we can test
	run_test_statement("drop table _test_btable");
	run_test_statement("create table _test_btable(id int, a int, b text, primary key(id))");
	run_test_statement("show tables");

	// simulate creating a local BTreeTable
	DbRelation& rel = SQLExec::test_get_tables().get_table("_test_btable");
	BTreeTable& table = *static_cast<BTreeTable*>(&rel);


	std::vector<ValueDict> rows;

	std::vector<Value> _a = { 12, -192, 1000 };
	std::vector<Value> _b = { "Hello!", "Much longer peice of text here", "" };	// TODO: make this long

	int size = _a.size();
	for (int id = 0; id < 10 * size; ++id)
	{
		rows.push_back(ValueDict{ { "id", Value(id) },
								  { "a", Value(_a[id % size]) },
								  { "b", Value(_b[id % size]) } });
	}



	Handles handles;
	std::vector<ValueDict> expected;
	for (ValueDict& row : rows)
	{
		expected.push_back(row);
		Handle ins_handle = table.insert(&row);
		handles.push_back(ins_handle);
	}

	std::cout << "Just inserted " << rows.size() << " records. Table contents: " << std::endl;
	run_test_statement("select * from _test_btable;");

	for (auto handle : (*table.select()))
	{
		ValueDict* row = table.project(handle);

		Value& current_projected_row_id_value = row->at("id");
		int cur_proj_row_id = current_projected_row_id_value.n;

		ValueDict& val_dict_orig_inserted = expected[cur_proj_row_id];
		Value& orig_inserted_row_id_value = val_dict_orig_inserted["id"];
		int orig_inserted_row_id = orig_inserted_row_id_value.n;

		if (cur_proj_row_id != orig_inserted_row_id)
		{
			return false;
		}
	}

	size_t last = rows.size();
	ValueDict actual_row = rows.at(last - 1);
	Handle del_handle;

	for (Handle handle : *table.select(&actual_row))
	{
		if (*table.project(handle) != actual_row)
		{
			return false;
		}
	}

	del_handle = table.select(&actual_row)->back();
	table.del(del_handle);
	last = rows.size();
	actual_row = rows.at(last - 1);

	for (Handle handle : *table.select(&actual_row))
	{
		if (*table.project(handle) != actual_row)
		{
			return false;
		}
	}

	del_handle = table.select(&actual_row)->back();
	table.del(del_handle);
	actual_row = rows.at(0);

	std::cout << " Deleting last handle: " << del_handle << std::endl;
	run_test_statement("select * from _test_btable");

	for (Handle handle : *table.select(&actual_row))
	{
		if (*table.project(handle) != actual_row)
		{
			return false;
		}
	}

	return true;
}
