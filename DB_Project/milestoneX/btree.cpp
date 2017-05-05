#include "btree.h"
#include <cassert>
#include <string>

BTreeIndex::BTreeIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique) :
	DbIndex(relation, name, key_columns, unique),
	closed(true), stat(nullptr), root(nullptr), file(relation.get_table_name() + "-" + this->name)
{
	build_key_profile();	//TODO write this one!!
	// TODO: throw an error if not unique
}

BTreeIndex::~BTreeIndex()
{
	if (stat)
	{
		delete stat;
	}
	if (root)
	{
		delete root;
	}
}

void BTreeIndex::create()
{
	file.create();
	stat = new BTreeStat(file, STAT, STAT + 1, key_profile);
	root = new BTreeLeaf(file, stat->get_root_id(), key_profile, true);
	closed = false;

	// build the index, add every row from relation into index
	Handles* handles = relation.select();
	for (auto handle : *handles)
	{
		insert(handle);
	}

	// TODO: delete handles?
}

void BTreeIndex::drop()
{
	file.drop();
}

void BTreeIndex::open()
{
	// Open existing index. 
	// (Enables: lookup, [range if supported], insert, delete, update.)
	if (closed)
	{
		file.open();
		stat = new BTreeStat(file, STAT, key_profile);
		if (stat->get_height() == 1)
		{
			root = new BTreeLeaf(file, stat->get_root_id(), key_profile, false);
		}
		else
		{
			root = new BTreeInterior(file, stat->get_root_id(), key_profile, false);
		}
		closed = false;
	}
}

void BTreeIndex::close()
{
	// Closes the index.
	// (Disables: lookup, [range if supported], insert, delete, update.)
	file.close();

	delete stat;
	stat = nullptr;

	delete root;
	root = nullptr;

	closed = true;
}

Handles* BTreeIndex::lookup(ValueDict* key) const
{
	// Find all the rows whose columns are equal to key. Assumes key is a 
	// dictionary whose keys are the column names in the index.Returns a list
	// of row handles.
	KeyValue* kk = tkey(key);
	uint height = stat->get_height();
	return _lookup(root, height, kk);

	//return _lookup(root, stat->get_height(), tkey(key));
	/*return self._lookup(self.root, self.stat.height, self._tkey(key))
	return nullptr;*/
}

Handles* BTreeIndex::range(ValueDict* min_key, ValueDict* max_key) const
{
	throw DbRelationError("range not implemented yet");
}

void BTreeIndex::insert(Handle handle)
{
	ValueDict row;

	// Insert a row with the given handle. Row must exist in relation already.
	// need only the column names from the given indices, hence the handle & key 
	KeyValue* key_value = tkey(relation.project(handle, &key_columns));
	Insertion split_root = _insert(root, stat->get_height(), key_value, handle);

	// if we split the root, grow the tree up one level
	if (!BTreeNode::insertion_is_none(split_root))
	{
		BTreeInterior* root_node = new BTreeInterior(file, 0, key_profile, true);
		//BTreeInterior* root_node = (BTreeInterior*)root;
		root_node->set_first(root->get_id());
		root_node->insert(&split_root.second, split_root.first);
		root_node->save();
		stat->set_root_id(root_node->get_id());
		stat->set_height(stat->get_height() + 1);
		stat->save();
		root = root_node;
	}
}

void BTreeIndex::del(Handle handle)
{
	throw DbRelationError("delete not implemented yet");
}

KeyValue* BTreeIndex::tkey(const ValueDict* key) const
{
	// Transform a key dictionary into a tuple in the correct order.
	KeyValue* value = new KeyValue;
	for (auto k : *key)
	{
		value->push_back(k.second);
	}
	return value;
}

void BTreeIndex::build_key_profile()
{
	ColumnAttributes* column_attributes = relation.get_column_attributes(key_columns);

	for (auto column_attribute : *column_attributes)
	{
		key_profile.push_back(column_attribute.get_data_type());
	}
	delete column_attributes;
}

Handles* BTreeIndex::_lookup(BTreeNode* node, uint height, const KeyValue* key) const
{
	// Recursive lookup
	Handles* handles = new Handles;
	//Handle handle;

	// if node is a leaf, return handle
	if (height == 1 /*leaf_node*/)
	{
		try
		{
			BTreeLeaf* leaf_node = (BTreeLeaf*)node;
			//handle = leaf_node->find_eq(key);
			handles->push_back(leaf_node->find_eq(key));
		}
		catch (std::out_of_range) {}

		return handles;
	}
	// otherwise, node is interior, recurse to next level
	else
	{
		BTreeInterior* interior_node = (BTreeInterior*)node;
		return _lookup(interior_node->find(key, height), height - 1, key);
	}
}

// Recursive insert. If a split happens at this level, return the (new node, boundary) of the split
Insertion BTreeIndex::_insert(BTreeNode* node, uint height, const KeyValue* key, Handle handle)
{
	Insertion insertion;

	//BTreeLeaf* leaf_node = dynamic_cast<BTreeLeaf*>(node);

	// base case: a leaf node
	if (height == 1 /*leaf_node*/)
	{
		try
		{
			BTreeLeaf* leaf_node = (BTreeLeaf*)node;
			insertion = leaf_node->insert(key, handle);
			leaf_node->save();
			//return BTreeNode::insertion_none();
		}
		catch (DbRelationError) {}	//TODO: what to do here?
		// returns insertion_none() or actual insertion as appropriate
		return insertion;
	}

	// recursive case: interior node
	BTreeInterior* interior_node = (BTreeInterior*)node;
	insertion = _insert(interior_node->find(key, height), height - 1, key, handle);

	if (!BTreeNode::insertion_is_none(insertion))
	{
		try
		{
			insertion = interior_node->insert(&insertion.second, insertion.first);
			interior_node->save();
		}
		catch (DbRelationError) {}	// TODO: what to do...
	}
	return insertion;
}

void print_table(HeapTable* table)
{
	Handles* x = table->select();
	ValueDicts* y = table->project(x);

	for (auto z : *y)
	{
		std::vector<std::map<std::basic_string<char>, Value>*>::value_type xxx = z;

		for (auto zz : *z)
		{
			std::cout << "row[\"" << zz.first << "\"] = " << std::to_string(zz.second.n)
				<< " inserted" << std::endl;
		}
	}
	std::cout << std::endl;
}

void print_table(HeapTable* table, int skip)
{
	int count = 0;
	Handles* x = table->select();
	ValueDicts* y = table->project(x);

	for (auto z : *y)
	{
		if (count % skip == 0)
		{
			std::vector<std::map<std::basic_string<char>, Value>*>::value_type xxx = z;

			for (auto zz : *z)
			{
				std::cout << "row[\"" << zz.first << "\"] = " << std::to_string(zz.second.n)
					<< " inserted" << std::endl;
			}
		}
		++count;
	}
	std::cout << std::endl;
}

bool test_btree()
{

	std::cout << "********************** BTree Test **********************" << std::endl;	//DELETE ME

	//ColumnNames column_names = { "a", "b" };
	ColumnNames column_names;
	column_names.push_back("a");
	column_names.push_back("b");

	//ColumnAttributes column_attributes = { ColumnAttribute::DataType::INT, ColumnAttribute::DataType::INT };
	ColumnAttributes column_attributes;
	ColumnAttribute column_attribute(ColumnAttribute::INT);
	column_attributes.push_back(column_attribute);
	column_attributes.push_back(column_attribute);
	HeapTable table("foo", column_names, column_attributes);
	table.create();
	ValueDict row1;
	row1["a"] = Value(12);
	row1["b"] = Value(99);
	table.insert(&row1);

	std::cout << "********* row1 test *********" << std::endl;
	print_table(&table);

	ValueDict row2;
	row2["a"] = Value(88);
	row2["b"] = Value(101);
	table.insert(&row2);
	//std::cout << "row1[\"a\"] = Value(88);\nrow1[\"b\"] = Value(101);\ninserted" << std::endl;

	std::cout << "********* row2 test *********" << std::endl;
	print_table(&table);

	ValueDict row;	// TODO: test that clear is working and not deleting everything
	for (int i = 0; i < 1000; ++i)
	{
		row["a"] = Value(i + 100);
		row["b"] = Value(-i);
		table.insert(&row);
		row.clear();
	}
	std::cout << "inserted 1000 rows" << std::endl;
	std::cout << "********* show one every 100 test *********" << std::endl;
	print_table(&table, 100);

	ColumnNames idx_column_names = { "a" };
	BTreeIndex index(table, "fooindex", idx_column_names, true);
	index.create();
	std::cout << "created a btree index..." << std::endl;

	ValueDict test;
	test["a"] = Value(12);
	Handles* handles = index.lookup(&test);
	ValueDicts results;
	for (auto handle : *handles)	// should only be one of these...
	{
		results.push_back(table.project(handle));
	}

	for (auto result : results)
	{
		if (test.at("a") != result->at("a"))
		{
			std::cout << "test.at(a) != result->at(a)" << std::endl;
		}
	}

	test.clear();
	handles->clear();
	results.clear();

	test["a"] = Value(88);
	handles = index.lookup(&test);
	for (auto handle : *handles)
	{
		results.push_back(table.project(handle));
		//assert(*result == row2);
	}

	for (auto result : results)
	{
		ValueDict::mapped_type buz = test.at("a");
		std::map<std::basic_string<char>, Value>::mapped_type boz = result->at("a");
		if (test.at("a") != result->at("a"))
		{
			std::cout << "test.at(a) != result->at(a)" << std::endl;
		}
	}

	test.clear();
	handles->clear();
	results.clear();

	row.clear();	// have an empty row TODO: check if redundant
	test["a"] = Value(6);
	handles = index.lookup(&test);
	for (auto handle : *handles)
	{
		results.push_back(table.project(handle));
		//assert(*result == row);		// this is going to break too
	}
	for (auto result : results)
	{
		ValueDict::mapped_type buz = test.at("a");
		std::map<std::basic_string<char>, Value>::mapped_type boz = result->at("a");
		if (test.at("a") != result->at("a"))
		{
			std::cout << "test.at(a) != result->at(a)" << std::endl;
		}
	}

	test.clear();
	row.clear();
	handles->clear();
	results.clear();
	for (int j = 0; j < 10; ++j)
	{
		for (int i = 0; i < 1000; ++i)
		{
			test["a"] = Value(i + 100);
			handles = index.lookup(&test);
			row["a"] = Value(i + 100);
			row["b"] = Value(-i);
			for (auto handle : *handles)
			{
				results.push_back(table.project(handle));
				//assert(*result == row);
			}
			for (auto result : results)
			{
				ValueDict::mapped_type buz = test.at("a");
				std::map<std::basic_string<char>, Value>::mapped_type boz = result->at("a");
				if (test.at("a") != result->at("a"))
				{
					std::cout << "test.at(a) != result->at(a)" << std::endl;
				}
			}

			test.clear();
			row.clear();
			handles->clear();
			results.clear();

			if (i % 100 == 0)
			{
				std::cout << "on the " << i*j << "th one...";
			}
		}
	}
	index.drop();
	table.drop();
	return true;
}


