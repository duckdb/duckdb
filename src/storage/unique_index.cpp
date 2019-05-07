#include "storage/unique_index.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "storage/data_table.hpp"
#include "transaction/transaction.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

UniqueIndex::UniqueIndex(DataTable &table, vector<TypeId> types, vector<uint64_t> keys, bool allow_nulls)
    : serializer(types, keys), comparer(serializer, table.serializer), table(table), types(types), keys(keys),
      allow_nulls(allow_nulls) {
}

UniqueIndexNode *UniqueIndex::AddEntry(Transaction &transaction, Tuple tuple, uint64_t row_identifier,
                                       unordered_set<uint64_t> &ignored_identifiers) {
	auto new_node = make_unique<UniqueIndexNode>(move(tuple), row_identifier);
	if (!root) {
		// no root, make this entry the root
		root = move(new_node);
		return root.get();
	}
	UniqueIndexNode *prev = nullptr, *entry = root.get();
	int cmp = 0;

	// traverse the tree
	while (entry) {
		// compare the new entry to the current entry
		cmp = serializer.Compare(entry->tuple, new_node->tuple);
		if (cmp == 0) {
			// node is potentially equivalent
			// check the base table for the actual version
			auto chunk = table.GetChunk(entry->row_identifier);
			assert(chunk);

			auto offset = entry->row_identifier - chunk->start;
			// whenever we call the AddEntry method we need a lock
			// on the last StorageChunk to guarantee that
			// the row identifiers are correct
			// for this reason locking it again here will cause a
			// deadlock. We only need to lock chunks that are not the last
			unique_ptr<StorageLock> lock;
			if (chunk != table.tail_chunk) {
				lock = chunk->GetSharedLock();
			}
			bool conflict = true;

			// check if this row is one of the rows that will be updated
			// if it is, we ignore the base table versions (as they will be
			// overwritten)
			if (ignored_identifiers.find(entry->row_identifier) == ignored_identifiers.end()) {
				// compare to base table version to verify there is a conflict
				if (chunk->deleted[offset]) {
					conflict = false;
				} else {
					// first serialize to tuple
					auto tuple_data = unique_ptr<uint8_t[]>{new uint8_t[chunk->table.serializer.TupleSize()]};

					chunk->table.serializer.Serialize(chunk->columns, offset, tuple_data.get());
					// now compare them
					// we use the TupleComparer because the tuple is serialized
					// from the base table
					conflict = comparer.Compare(new_node->tuple.data.get(), tuple_data.get()) == 0;
				}
			} else {
				conflict = false;
			}

			// check the version number if there was no conflict
			auto version = chunk->version_pointers[offset];
			if (version && !conflict) {
				if (!(version->version_number == transaction.transaction_id ||
				      version->version_number < transaction.start_time)) {
					// we don't have to use the base table version, keep going
					// until we reach the version that belongs to this entry
					while (true) {
						if (!version->tuple_data) {
							// our entry was deleted, no conflict
							conflict = false;
						} else {
							conflict = comparer.Compare(new_node->tuple.data.get(), version->tuple_data) == 0;
							if (conflict) {
								break;
							}
						}

						auto next = version->next;
						if (!next) {
							// use this version: no predecessor
							break;
						}
						if (next->version_number == transaction.transaction_id) {
							// use this version: it was created by us
							break;
						}
						if (next->version_number < transaction.start_time) {
							// use this version: it was committed by us
							break;
						}
						version = next;
					}
				}
			}

			if (conflict) {
				return nullptr;
			}
		}

		prev = entry;
		if (cmp < 0) {
			// smaller: go to the left
			entry = entry->left.get();
		} else {
			// greater or equal: go to the right
			entry = entry->right.get();
		}
	}

	// we can place the node in the tree now
	new_node->parent = prev;
	if (cmp < 0) {
		// left side
		prev->left = move(new_node);
		return prev->left.get();
	} else {
		// right side
		prev->right = move(new_node);
		return prev->right.get();
	}
}

void UniqueIndex::RemoveEntry(UniqueIndexNode *entry) {
	if (!entry->parent) {
		// root node
		// move root to either left or right
		if (entry->left && entry->right) {
			// we place (entry->left) at root
			// and (entry->right) as the right-child of the root
			auto right_node = move(entry->right);
			root = move(entry->left);

			if (root->right) {
				// if (entry->left) already has a right child we move it to the
				// left-most position of the right node
				auto n = right_node.get();
				while (n->left) {
					n = n->left.get();
				}
				n->left = move(root->right);
			}
			root->right = move(right_node);
		} else if (entry->left) {
			root = move(entry->left);
		} else if (entry->right) {
			root = move(entry->right);
		} else {
			// empty tree
			root = nullptr;
		}
	} else {
		if (!entry->left && !entry->right) {
			// no children
			// we only need to remove the node from the parent
			auto parent = entry->parent;
			if (entry == parent->left.get()) {
				parent->left.reset();
			} else {
				parent->right.reset();
			}
		} else {
			// we don't handle this case yet!
			assert(0);
		}
	}
}

void UniqueIndex::AddEntries(Transaction &transaction, UniqueIndexAddedEntries &nodes, Tuple tuples[], bool has_null[],
                             Vector &row_identifiers, unordered_set<uint64_t> &ignored_identifiers) {

	lock_guard<mutex> guard(index_lock);

	assert(row_identifiers.type == TypeId::BIGINT);

	auto identifiers = (int64_t *)row_identifiers.data;
	for (uint64_t i = 0; i < row_identifiers.count; i++) {
		auto row_identifier = row_identifiers.sel_vector ? identifiers[row_identifiers.sel_vector[i]] : identifiers[i];
		UniqueIndexNode *entry = nullptr;
		if (has_null[i]) {
			if (allow_nulls) {
				// skip entries with NULL values
				continue;
			} else {
				// if NULLs are not allowed, throw an exception
				throw ConstraintException("PRIMARY KEY column cannot contain NULL values!");
			}
		} else {
			entry = AddEntry(transaction, move(tuples[i]), row_identifier, ignored_identifiers);
			if (!entry) {
				// could not add entry: constraint violation
				throw ConstraintException("PRIMARY KEY or UNIQUE constraint violated: duplicated key");
			}
		}
		nodes.AddEntry(entry);
	}
}

static bool boolean_array_from_nullmask(sel_t *sel_vector, uint64_t count, nullmask_t &mask, bool array[],
                                        bool allow_nulls) {
	bool success = true;
	VectorOperations::Exec(sel_vector, count, [&](uint64_t i, uint64_t k) {
		array[k] = mask[i];
		if (array[k] && !allow_nulls) {
			success = false;
			return;
		}
	});
	return success;
}

void UniqueIndex::Append(Transaction &transaction, vector<unique_ptr<UniqueIndex>> &indexes, DataChunk &chunk,
                         uint64_t row_identifier_start) {
	if (indexes.size() == 0) {
		return;
	}

	// the row numbers that are ignored for conflicts
	// this is left empty because we ignore nothing in the append
	unordered_set<uint64_t> dummy;

	// we keep a set of nodes we added to the different indexes
	// we keep this set so we can remove them from the index again in case of a constraint violation
	unique_ptr<UniqueIndexAddedEntries> chain;
	// insert the entries in each of the unique indexes
	for (uint64_t current_index = 0; current_index < indexes.size(); current_index++) {
		auto &index = *indexes[current_index];
		auto added_nodes = make_unique<UniqueIndexAddedEntries>(index);

		Tuple tuples[STANDARD_VECTOR_SIZE];
		bool has_null[STANDARD_VECTOR_SIZE];

		nullmask_t nulls;
		for (auto &key : index.keys) {
			nulls |= chunk.data[key].nullmask;
		}
		if (!boolean_array_from_nullmask(chunk.sel_vector, chunk.size(), nulls, has_null, index.allow_nulls)) {
			throw ConstraintException("PRIMARY KEY column cannot contain NULL values!");
		}
		index.serializer.Serialize(chunk, tuples);

		// create the row number vector
		Vector row_numbers(TypeId::BIGINT, true, false);
		row_numbers.count = chunk.size();
		VectorOperations::GenerateSequence(row_numbers, row_identifier_start);

		// now actually add the entries to this index
		index.AddEntries(transaction, *added_nodes, tuples, has_null, row_numbers, dummy);

		added_nodes->next = move(chain);
		chain = move(added_nodes);
	}
	// we succeeded, so we don't delete anything from the index anymore
	chain->Flush();
}

// Handle an update in the UniqueIndex
// Handling updates is a bit more difficult than handling an Append because we
// do it in bulk and if we simply treat the entries as an append there might be
// conflicts Consider for example if we do the query "UPDATE integers SET
// i=i+1"; This will never cause a conflict to happen, but might cause a fake
// conflict if we add the entries to the list in bulk, because {10, 11} -> {11,
// 12}: 11 already exists ERROR For this reason we check separately for
// conflicts WITHIN the update_chunk and ignore any of these entries when
// checking inside the actual index
void UniqueIndex::Update(Transaction &transaction, StorageChunk *storage, vector<unique_ptr<UniqueIndex>> &indexes,
                         vector<column_t> &updated_columns, DataChunk &update_chunk, Vector &row_identifiers) {
	if (indexes.size() == 0) {
		return;
	}

	unordered_set<uint64_t> ignored_entries;

	VectorOperations::ExecType<uint64_t>(
	    row_identifiers, [&](uint64_t &entry, uint64_t i, uint64_t k) { ignored_entries.insert(entry); });

	// we keep a set of nodes we added to the different indexes
	// we keep this set so we can remove them from the index again in case of a constraint violation
	unique_ptr<UniqueIndexAddedEntries> chain;
	for (uint64_t current_index = 0; current_index < indexes.size(); current_index++) {
		auto &index = *indexes[current_index];
		auto added_nodes = make_unique<UniqueIndexAddedEntries>(index);

		// first check if the updated columns affect the index
		bool index_affected = false;
		vector<column_t> affected_columns;
		for (uint64_t i = 0; i < index.keys.size(); i++) {
			auto column = index.keys[i];
			auto entry = find(updated_columns.begin(), updated_columns.end(), column);
			if (entry != updated_columns.end()) {
				affected_columns.push_back(entry - updated_columns.begin());
				index_affected = true;
			} else {
				affected_columns.push_back((column_t)-1);
			}
		}
		if (!index_affected) {
			// if not, we can ignore the update
			continue;
		}

		// otherwise we need to add the update to the index and verify that
		// there are no conflicts. First we serialize the updates to a set of
		// tuples. This is a bit more difficult as the update_chunk might not
		// contain all columns instead, we might have to go back to the base
		// table.
		Tuple tuples[STANDARD_VECTOR_SIZE];
		// first handle any NULL values
		bool has_null[STANDARD_VECTOR_SIZE];

		nullmask_t nulls;
		for (uint64_t i = 0; i < index.keys.size(); i++) {
			if (affected_columns[i] != (column_t)-1) {
				nulls |= update_chunk.data[affected_columns[i]].nullmask;
			}
		}
		if (!boolean_array_from_nullmask(update_chunk.sel_vector, update_chunk.size(), nulls, has_null,
		                                 index.allow_nulls)) {
			throw ConstraintException("PRIMARY KEY column cannot contain NULL values!");
		}

		index.serializer.SerializeUpdate(storage->columns, affected_columns, update_chunk, row_identifiers,
		                                 storage->start, tuples);

		// check if there are duplicates in the tuples themselves
		TupleSet set;
		for (uint64_t i = 0; i < update_chunk.size(); i++) {
			TupleReference ref(&tuples[i], index.serializer);
			auto entry = set.find(ref);
			if (entry != set.end()) {
				throw ConstraintException("PRIMARY KEY or UNIQUE constraint violated: duplicated key");
			}
			set.insert(ref);
		}

		// now actually add the entries to this index
		index.AddEntries(transaction, *added_nodes, tuples, has_null, row_identifiers, ignored_entries);
		added_nodes->next = move(chain);
		chain = move(added_nodes);
	}
	// we succeeded, so we don't delete anything from the index anymore
	if (chain) {
		chain->Flush();
	}
}
