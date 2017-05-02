#include "btree.h"

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
	//return tuple(key[column_name] for column_name in self.key)
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

Insertion BTreeIndex::_insert(BTreeNode* node, uint height, const KeyValue* key, Handle handle)
{
	return {};
}

bool test_btree()
{
	return false;
}


