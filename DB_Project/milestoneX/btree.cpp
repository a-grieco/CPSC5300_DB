#include "btree.h"
#include <cassert>

BTreeIndex::BTreeIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique) :
	DbIndex(relation, name, key_columns, unique),
	closed(true), stat(nullptr), root(nullptr), file(relation.get_table_name() + "-" + this->name)
{
	build_key_profile();
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
	return _lookup(root, stat->get_height(), tkey(key));
	/*return self._lookup(self.root, self.stat.height, self._tkey(key))
	return nullptr;*/
}

Handles* BTreeIndex::range(ValueDict* min_key, ValueDict* max_key) const
{
	throw DbRelationError("range not implemented yet");
}

void BTreeIndex::insert(Handle handle)
{
	// Insert a row with the given handle. Row must exist in relation already.
	KeyValue* value = tkey(relation.project(handle));	// DELETE - why (handle, key) in Python

	Insertion split_root = _insert(root, stat->get_height(), value, handle);

	// if we split the root, grow the tree up one level
	if (!BTreeNode::insertion_is_none(split_root))
	{
		root = new BTreeInterior(file, 0, key_profile, true);
		BTreeInterior* root_node = dynamic_cast<BTreeInterior*>(root);
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
}

Handles* BTreeIndex::_lookup(BTreeNode* node, uint height, const KeyValue* key) const
{
	// Recursive lookup
	Handles* handles = new Handles;
	BTreeLeaf* leaf_node = dynamic_cast<BTreeLeaf*>(node);

	// if node is a leaf, return handle
	if (leaf_node)
	{
		Handle handle = leaf_node->find_eq(key);
		handles->push_back(handle);
		return handles;
	}
	// otherwise, node is interior, recurse to next level
	else
	{
		BTreeInterior* interior_node = dynamic_cast<BTreeInterior*>(node);
		if (interior_node)
		{
			return _lookup(interior_node->find(key, height), height - 1, key);
		}
		else
		{
			throw DbRelationError("not recognized as interior or leaf node");
		}
	}
}

// Recursive insert. If a split happens at this level, return the (new node, boundary) of the split
Insertion BTreeIndex::_insert(BTreeNode* node, uint height, const KeyValue* key, Handle handle)
{
	Insertion insertion;

	// base case: a leaf node
	BTreeLeaf* leaf_node = dynamic_cast<BTreeLeaf*>(node);
	if (leaf_node)
	{
		// returns insertion_none() or actual insertion
		insertion = leaf_node->insert(key, handle);
		if (BTreeNode::insertion_is_none(insertion))
		{
			leaf_node->save();
		}
		return insertion;
	}
	// recursive case: interior node
	BTreeInterior* interior_node = dynamic_cast<BTreeInterior*>(node);
	if (interior_node)
	{
		//Insertion new_kid = _insert(interior_node->find(key, height), height - 1, key, handle);
		insertion = _insert(interior_node->find(key, height), height - 1, key, handle);

		if (!BTreeNode::insertion_is_none(insertion))
		{
			insertion = interior_node->insert(&insertion.second, insertion.first);
			if (BTreeNode::insertion_is_none(insertion))
			{
				interior_node->save();
			}
			return insertion;	// TODO: fix whatever this is
		}
		return insertion;
	}
	// if node not regognized as root or interior, there is a problem
	throw DbRelationError("node not recognized as root or interior");
}

bool test_btree()
{
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
	table.create_if_not_exists();
	ValueDict row1;
	row1["a"] = Value(12);
	row1["b"] = Value(99);
	table.insert(&row1);
	ValueDict row2;
	row2["a"] = Value(88);
	row2["b"] = Value(101);
	table.insert(&row2);
	ValueDict row;	// TODO: test that clear is working and not deleting everything
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
	ValueDict* result = nullptr;
	for (auto handle : *handles)	// should only be one of these...
	{
		result = table.project(handle);
		assert(*result == row1);	// this will most likely break
	}

	test.clear();
	handles->clear();
	result->clear();

	test["a"] = Value(88);
	handles = index.lookup(&test);
	for (auto handle : *handles)
	{
		result = table.project(handle);
		assert(*result == row2);
	}

	test.clear();
	handles->clear();
	result->clear();

	row.clear();	// have an empty row TODO: check if redundant
	test["a"] = Value(6);
	handles = index.lookup(&test);
	for (auto handle : *handles)
	{
		result = table.project(handle);
		assert(*result == row);		// this is going to break too
	}

	test.clear();
	row.clear();
	handles->clear();
	result->clear();
	for (int j = 0; j < 10; ++j)
	{
		for(int i = 0; i <1000; ++i)
		{
			test["a"] = Value(i + 100);
			handles = index.lookup(&test);
			row["a"] = Value(i + 100);
			row["b"] = Value(-i);
			for(auto handle : *handles)
			{
				result = table.project(handle);
				assert(*result == row);
			}
			test.clear();
			row.clear();
			handles->clear();
			result->clear();
		}
	}

	table.drop();
	return true;
}


