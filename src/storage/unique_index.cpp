
#include "common/exception.hpp"

#include "storage/data_table.hpp"
#include "storage/unique_index.hpp"

#include "transaction/transaction.hpp"

using namespace duckdb;
using namespace std;

UniqueIndex::UniqueIndex(DataTable &table, std::vector<TypeId> types,
                         std::vector<size_t> keys, bool allow_nulls)
    : table(table), types(types), keys(keys), allow_nulls(allow_nulls) {
	assert(types.size() == keys.size());
	for (size_t i = 0; i < keys.size(); i++) {
		auto type = types[i];
		if (!TypeIsConstantSize(type)) {
			assert(type == TypeId::VARCHAR);
			variable_columns.push_back(keys[i]);
		} else {
			base_size += GetTypeIdSize(type);
		}
	}
}

UniqueIndexNode *UniqueIndex::AddEntry(Transaction &transaction, size_t size,
                                       std::unique_ptr<uint8_t[]> data,
                                       size_t row_identifier) {
	auto new_node =
	    make_unique<UniqueIndexNode>(size, move(data), row_identifier);
	if (!root) {
		// no root, make this entry the root
		root = move(new_node);
		return root.get();
	}
	UniqueIndexNode *prev = nullptr, *entry = root.get();
	int cmp = 0;
	// traverse the tree
	while (entry) {
		auto min_size = std::min(size, entry->size);

		// compare the new entry to the current entry
		cmp = memcmp(entry->key_data.get(), new_node->key_data.get(), min_size);

		if (cmp == 0 && size == entry->size) {
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
			auto version = chunk->version_pointers[offset];
			// if (version) {
			// 	// first check if we need to use the base table entry
			// 	// compare to base table version
			// 	if (chunk->deleted[offset]) {
			// 		conflict = false;
			// 	} else {
			// 		assert(0);
			// 		// do actual comparison
			// 	}

			// 	if (!conflict && !(version->version_number ==
			// transaction.transaction_id  || 	     version->version_number <
			// transaction.start_time)) {
			// 		// we don't have to use the base table version, keep going
			// 		// until we reach the version that belongs to this entry
			// 		while(true) {
			// 			if (!version->tuple_data) {
			// 				// our entry was deleted, no conflict
			// 				conflict = false;
			// 			} else {
			// 				assert(0);
			// 				if (conflict) {
			// 					break;
			// 				}
			// 			}

			// 			auto next = version->next;
			// 			if (!next) {
			// 				// use this version: no predecessor
			// 				break;
			// 			}
			// 			if (next->version_number ==
			// 			    transaction.transaction_id) {
			// 				// use this version: it was created by us
			// 				break;
			// 			}
			// 			if (next->version_number < transaction.start_time) {
			// 				// use this version: it was committed by us
			// 				break;
			// 			}
			// 			version = next;
			// 		}
			// 	}
			// }

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

string UniqueIndex::Append(Transaction &transaction,
                           vector<unique_ptr<UniqueIndex>> &indexes,
                           DataChunk &chunk, size_t row_identifier_start) {
	if (indexes.size() == 0) {
		return string();
	}

	UniqueIndexNode *added_nodes[indexes.size()][STANDARD_VECTOR_SIZE];

	string error;
	bool success = true;
	size_t current_index = 0;
	// insert the entries in each of the unique indexes
	for (current_index = 0; current_index < indexes.size(); current_index++) {
		auto &index = *indexes[current_index];

		size_t key_size[STANDARD_VECTOR_SIZE];
		unique_ptr<uint8_t[]> key_data[STANDARD_VECTOR_SIZE];
		bool has_null[STANDARD_VECTOR_SIZE];

		for (size_t i = 0; i < chunk.count; i++) {
			size_t entry = chunk.sel_vector ? chunk.sel_vector[i] : i;
			key_size[i] = index.base_size;
			for (auto &key : index.variable_columns) {
				assert(chunk.data[key].type == TypeId::VARCHAR);
				char **string_data = (char **)chunk.data[key].data;
				key_size[i] +=
				    string_data[entry] ? strlen(string_data[entry]) + 1 : 0;
			}
			key_data[i] = unique_ptr<uint8_t[]>(new uint8_t[key_size[i]]);
			has_null[i] = false;

			// copy the data
			char *tuple_data = (char *)key_data[i].get();
			for (size_t j = 0; j < index.keys.size(); j++) {
				auto key = index.keys[j];
				assert(index.types[j] == chunk.data[key].type);
				if (chunk.data[key].nullmask[i]) {
					if (index.allow_nulls) {
						// any key that has a NULL value we can skip placing in
						// the index entirely because NULL values are always <>
						// to NULL values, any key with a NULL value can ALWAYS
						// be placed inside the index
						key_data[i].reset();
						has_null[i] = true;
						break;
					} else {
						// if NULLs are not allowed, throw an exception
						error =
						    "PRIMARY KEY column cannot contain NULL values!";
						success = false;
						break;
					}
				}
				if (TypeIsConstantSize(index.types[j])) {
					auto data_size = GetTypeIdSize(index.types[j]);
					memcpy(tuple_data, chunk.data[key].data + data_size * entry,
					       data_size);
					tuple_data += data_size;
				} else {
					const char **string_data =
					    (const char **)chunk.data[key].data;
					strcpy(tuple_data, string_data[entry]);
					tuple_data += strlen(string_data[entry]) + 1;
				}
			}
			if (!success) {
				break;
			}
		}
		if (!success) {
			break;
		}

		lock_guard<mutex> guard(index.index_lock);
		// now actually add the entries to this index
		for (size_t i = 0; i < chunk.count; i++) {
			if (has_null[i]) {
				// skip entries with NULL values
				added_nodes[current_index][i] = nullptr;
				continue;
			}

			auto entry =
			    index.AddEntry(transaction, key_size[i], move(key_data[i]),
			                   row_identifier_start + i);
			if (!entry) {
				// could not add entry: constraint violation
				error =
				    "PRIMARY KEY or UNIQUE constraint violated: duplicated key";
				// remove all added entries from this index
				for (size_t j = i; j > 0; j--) {
					if (added_nodes[current_index][j - 1]) {
						index.RemoveEntry(added_nodes[current_index][j - 1]);
					}
				}
				// break out of the loop
				success = false;
				break;
			}
			added_nodes[current_index][i] = entry;
		}
		if (!success) {
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
