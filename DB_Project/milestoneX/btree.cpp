#include "btree.h"
#include <cassert>
#include <string>
#include "scopeguard.h"

BTreeIndex::BTreeIndex(DbRelation& relation, Identifier name,
	ColumnNames key_columns, bool unique) :
	DbIndex(relation, name, key_columns, unique), closed(true), stat(nullptr),
	root(nullptr), file(relation.get_table_name() + "-" + this->name)
{
	build_key_profile();

	if (!unique)
	{
		throw std::invalid_argument("BTreeIndex must be on a unique search key");
	}
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
	Handles* handles_iserted = new Handles;
	try
	{
		for (auto handle : *handles)
		{
			insert(handle);
			handles_iserted->push_back(handle);
		}
	}
	catch (DbRelationError)
	{
		file.drop();
		throw;
	}
	delete handles;
	delete handles_iserted;
}

void BTreeIndex::drop()
{
	file.drop();
}

// Open existing index. (Enables: lookup, insert, delete, update.)
void BTreeIndex::open()
{
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

// Closes the index. (Disables: lookup, insert, delete, update.)
void BTreeIndex::close()
{
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
	// dictionary whose keys are the column names in the index.
	return _lookup(root, stat->get_height(), tkey(key));
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

	// if node is a leaf, return handle
	if (height == 1)
	{
		try
		{
			BTreeLeaf* leaf_node = (BTreeLeaf*)node;
			handles->push_back(leaf_node->find_eq(key));
		}
		catch (std::out_of_range) {}	// handles will be empty

		return handles;
	}
	// otherwise, node is interior, recurse to next level
	else
	{
		BTreeInterior* interior_node = (BTreeInterior*)node;
		return _lookup(interior_node->find(key, height), height - 1, key);
	}
}

// Recursive insert. If a split happens at this level, return the 
// (new node, boundary) of the split
Insertion BTreeIndex::_insert(BTreeNode* node, uint height,
	const KeyValue* key, Handle handle)
{
	Insertion insertion;

	// base case: a leaf node
	if (height == 1 /*leaf_node*/)
	{
		BTreeLeaf* leaf_node = (BTreeLeaf*)node;
		insertion = leaf_node->insert(key, handle);
		leaf_node->save();
		return insertion;
	}

	// recursive case: interior node
	BTreeInterior* interior_node = (BTreeInterior*)node;
	insertion = _insert(interior_node->find(key, height), height - 1, key, handle);

	if (!BTreeNode::insertion_is_none(insertion))
	{
		insertion = interior_node->insert(&insertion.second, insertion.first);
		interior_node->save();
	}
	return insertion;
}

// returns true if the results match the row being tested; otherwise false
bool test_passes(HeapTable* table, Handles* handles, ValueDicts results,
	const ValueDict& row)
{
	for (auto handle : *handles)
	{
		results.push_back(table->project(handle));
	}

	for (auto result : results)
	{
		if (row.at("a").n != result->at("a").n ||
			row.at("b").n != result->at("b").n)
		{
			return false;
		}
	}
	return true;
}

bool test_btree()
{
	ColumnNames column_names = { "a", "b" };
	ColumnAttributes column_attributes =
	{ ColumnAttribute::DataType::INT, ColumnAttribute::DataType::INT };

	HeapTable table("foo", column_names, column_attributes);
	table.create();

	ValueDict row1;
	row1["a"] = Value(12);
	row1["b"] = Value(99);
	table.insert(&row1);

	ValueDict row2;
	row2["a"] = Value(88);
	row2["b"] = Value(101);
	table.insert(&row2);

	ValueDict row;
	for (int i = 0; i < 1000; ++i)
	{
		row["a"] = Value(i + 100);
		row["b"] = Value(-i);
		table.insert(&row);
		row.clear();
	}

	ColumnNames idx_column_names = { "a" };
	BTreeIndex index(table, "fooindex", idx_column_names, true);
	index.create();

	ValueDict test;
	test["a"] = Value(12);
	Handles* handles = index.lookup(&test);
	ValueDicts results;

	if (!test_passes(&table, handles, results, row1))
	{
		std::cout << "row1 test failed" << std::endl;
		return false;
	}

	test.clear();
	handles->clear();
	results.clear();

	test["a"] = Value(88);
	handles = index.lookup(&test);

	if (!test_passes(&table, handles, results, row2))
	{
		std::cout << "row2 test failed" << std::endl;
		return false;
	}

	test.clear();
	handles->clear();
	results.clear();

	// this test should not return any matching values
	test["a"] = Value(6);
	handles = index.lookup(&test);

	if (!test_passes(&table, handles, results, row))
	{
		std::cout << "unmatched test failed" << std::endl;
		return false;
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

			if (!test_passes(&table, handles, results, row))
			{
				std::cout << "test for row[\"a\"] = Value(" << i + 100 <<
					"] and row[\"b\"] = Value(" << -i << "] failed\n" <<
					"where j = " << j << " and i = " << i << std::endl;
				return false;
			}

			test.clear();
			row.clear();
			handles->clear();
			results.clear();
		}
	}

	index.drop();
	table.drop();
	return true;
}
