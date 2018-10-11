
#include "common/exception.hpp"
#include "common/types/vector_operations.hpp"

#include "storage/data_table.hpp"
#include "storage/unique_index.hpp"

#include "transaction/transaction.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

UniqueIndex::UniqueIndex(DataTable &table, std::vector<TypeId> types,
                         std::vector<size_t> keys, bool allow_nulls)
    : serializer(types, false, keys), comparer(serializer, table.serializer),
      table(table), types(types), keys(keys), allow_nulls(allow_nulls) {}

UniqueIndexNode *
UniqueIndex::AddEntry(Transaction &transaction, Tuple tuple,
                      size_t row_identifier,
                      unordered_set<size_t> &ignored_identifiers) {
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
			auto offset = entry->row_identifier - chunk->start;
			// whenever we call the AddEntry method we need a lock
			// on the last StorageChunk to guarantee that
			// the row identifiers are correct
			// for this reason locking it again here will cause a
			// deadlock. We only need to lock chunks that are not the last
			if (chunk != table.tail_chunk) {
				chunk->GetSharedLock();
			}
			bool conflict = true;

			// check if this row is one of the rows that will be updated
			// if it is, we ignore the base table versions (as they will be
			// overwritten)
			if (ignored_identifiers.find(entry->row_identifier) ==
			    ignored_identifiers.end()) {
				// compare to base table version to verify there is a conflict
				if (chunk->deleted[offset]) {
					conflict = false;
				} else {
					// first serialize to tuple
					uint8_t tuple_data[chunk->table.serializer.TupleSize()];
					chunk->table.serializer.Serialize(chunk->columns, offset,
					                                  tuple_data);
					// now compare them
					// we use the TupleComparer because the tuple is serialized
					// from the base table
					conflict = comparer.Compare(new_node->tuple.data.get(),
					                            tuple_data) == 0;
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
							conflict =
							    comparer.Compare(new_node->tuple.data.get(),
							                     version->tuple_data) == 0;
							if (conflict) {
								break;
							}
						}

						auto next = version->next;
						if (!next) {
							// use this version: no predecessor
							break;
						}
						if (next->version_number ==
						    transaction.transaction_id) {
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
				if (chunk != table.tail_chunk) {
					chunk->ReleaseSharedLock();
				}
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

string UniqueIndex::AddEntries(Transaction &transaction,
                               UniqueIndexNode *added_nodes[], Tuple tuples[],
                               bool has_null[], Vector &row_identifiers,
                               unordered_set<size_t> &ignored_identifiers) {
	string error;

	lock_guard<mutex> guard(index_lock);

	assert(row_identifiers.type == TypeId::POINTER);

	auto identifiers = (uint64_t *)row_identifiers.data;
	for (size_t i = 0; i < row_identifiers.count; i++) {
		auto row_identifier = row_identifiers.sel_vector
		                          ? identifiers[row_identifiers.sel_vector[i]]
		                          : identifiers[i];
		UniqueIndexNode *entry = nullptr;
		if (has_null[i]) {
			if (allow_nulls) {
				// skip entries with NULL values
				added_nodes[i] = nullptr;
				continue;
			} else {
				// if NULLs are not allowed, throw an exception
				error = "PRIMARY KEY column cannot contain NULL values!";
			}
		} else {
			entry = AddEntry(transaction, move(tuples[i]), row_identifier,
			                 ignored_identifiers);
			if (!entry) {
				// could not add entry: constraint violation
				error = "PRIMARY KEY or UNIQUE constraint violated: "
				        "duplicated key";
			}
		}

		if (!entry) {
			// remove all added entries from this index and return the error
			for (size_t j = i; j > 0; j--) {
				if (added_nodes[j - 1]) {
					RemoveEntry(added_nodes[j - 1]);
				}
			}
			return error;
		}
		added_nodes[i] = entry;
	}
	return error;
}

string UniqueIndex::Append(Transaction &transaction,
                           vector<unique_ptr<UniqueIndex>> &indexes,
                           DataChunk &chunk, size_t row_identifier_start) {
	if (indexes.size() == 0) {
		return string();
	}

	UniqueIndexNode *added_nodes[indexes.size()][STANDARD_VECTOR_SIZE];

	// the row numbers that are ignored for conflicts
	// this is left empty because we ignore nothing in the append
	unordered_set<size_t> dummy;

	string error;
	size_t current_index = 0;
	// insert the entries in each of the unique indexes
	for (current_index = 0; current_index < indexes.size(); current_index++) {
		auto &index = *indexes[current_index];

		Tuple tuples[STANDARD_VECTOR_SIZE];
		bool has_null[STANDARD_VECTOR_SIZE] = {0};

		index.serializer.Serialize(chunk, tuples, has_null);

		// create the row number vector
		Vector row_numbers(TypeId::POINTER, true, false);
		row_numbers.count = chunk.count;
		VectorOperations::GenerateSequence(row_numbers, row_identifier_start);

		// now actually add the entries to this index
		error = index.AddEntries(transaction, added_nodes[current_index],
		                         tuples, has_null, row_numbers, dummy);

		if (!error.empty()) {
			break;
		}
	}
	if (current_index != indexes.size()) {
		// something went wrong! we have to revert all the additions
		for (size_t k = current_index; k > 0; k--) {
			auto &index = *indexes[k - 1];
			for (size_t j = chunk.count; j > 0; j--) {
				if (added_nodes[j - 1]) {
					index.RemoveEntry(added_nodes[k - 1][j - 1]);
				}
			}
		}
		return error;
	}
	return string();
}

string UniqueIndex::Update(Transaction &transaction, StorageChunk *storage,
                           std::vector<std::unique_ptr<UniqueIndex>> &indexes,
                           std::vector<column_t> &updated_columns,
                           DataChunk &update_chunk, Vector &row_identifiers) {
	if (indexes.size() == 0) {
		return string();
	}

	string error;
	size_t current_index;
	UniqueIndexNode *added_nodes[indexes.size()][STANDARD_VECTOR_SIZE];
	size_t added_nodes_count[indexes.size()];
	unordered_set<size_t> ignored_entries;

	VectorOperations::ExecType<uint64_t>(row_identifiers, [&](uint64_t &entry) {
		ignored_entries.insert(entry);
	});

	for (current_index = 0; current_index < indexes.size(); current_index++) {
		auto &index = *indexes[current_index];

		added_nodes_count[current_index] = 0;

		// first check if the updated columns affect the index
		bool index_affected = false;
		vector<column_t> affected_columns;
		for (size_t i = 0; i < index.keys.size(); i++) {
			auto column = index.keys[i];
			auto entry =
			    find(updated_columns.begin(), updated_columns.end(), column);
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
		// there are no conflicts first we serialize the updates to a set of
		// tuples this is a bit more difficult as the update_chunk might not
		// contain all columns instead, we might have to go back to the base
		// table
		Tuple tuples[STANDARD_VECTOR_SIZE];
		bool has_null[STANDARD_VECTOR_SIZE] = {0};

		index.serializer.SerializeUpdate(storage->columns, affected_columns,
		                                 update_chunk, row_identifiers,
		                                 storage->start, tuples, has_null);

		// check if there are duplicates in the tuples themselves
		// FIXME: this should use a hash set or a tree
		for (size_t i = 0; i < update_chunk.count; i++) {
			for (size_t j = i + 1; j < update_chunk.count; j++) {
				if (index.serializer.Compare(tuples[i], tuples[j]) == 0) {
					error = "PRIMARY KEY or UNIQUE constraint violated: "
					        "duplicated key";
					break;
				}
			}
			if (!error.empty()) {
				break;
			}
		}
		if (!error.empty()) {
			break;
		}

		// now actually add the entries to this index
		error =
		    index.AddEntries(transaction, added_nodes[current_index], tuples,
		                     has_null, row_identifiers, ignored_entries);
		if (!error.empty()) {
			break;
		}
		added_nodes_count[current_index] = update_chunk.count;
	}
	if (current_index != indexes.size()) {
		// something went wrong! we have to revert all the additions
		for (size_t k = current_index; k > 0; k--) {
			auto &index = *indexes[k - 1];
			for (size_t j = added_nodes_count[current_index]; j > 0; j--) {
				if (added_nodes[j - 1]) {
					index.RemoveEntry(added_nodes[k - 1][j - 1]);
				}
			}
		}
		return error;
	}
	return string();
}
