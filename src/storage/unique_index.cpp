
#include "storage/unique_index.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

UniqueIndex::UniqueIndex(std::vector<TypeId> types, std::vector<size_t> keys,
                         bool allow_nulls)
    : types(types), keys(keys), allow_nulls(allow_nulls) {
	for (size_t key : keys) {
		assert(key < types.size());
		auto type = types[key];
		if (!TypeIsConstantSize(type)) {
			assert(type == TypeId::VARCHAR);
			variable_columns.push_back(key);
		} else {
			base_size += GetTypeIdSize(type);
		}
	}
}

UniqueIndexNode *UniqueIndex::AddEntry(size_t size,
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
			// node is equivalent: return nullptr
			return nullptr;
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

void UniqueIndex::Append(DataChunk &chunk, size_t row_identifier_start) {
	size_t key_size[STANDARD_VECTOR_SIZE];
	unique_ptr<uint8_t[]> key_data[STANDARD_VECTOR_SIZE];
	bool has_null[STANDARD_VECTOR_SIZE];

	for (size_t i = 0; i < chunk.count; i++) {
		size_t index = chunk.sel_vector ? chunk.sel_vector[i] : i;
		key_size[i] = base_size;
		for (auto &key : variable_columns) {
			assert(chunk.data[key].type == TypeId::VARCHAR);
			char **string_data = (char **)chunk.data[key].data;
			key_size[i] +=
			    string_data[index] ? strlen(string_data[index]) + 1 : 0;
		}
		key_data[i] = unique_ptr<uint8_t[]>(new uint8_t[key_size[i]]);
		has_null[i] = false;

		// copy the data
		char *tuple_data = (char *)key_data[i].get();
		for (size_t key : keys) {
			if (chunk.data[key].nullmask[i]) {
				if (allow_nulls) {
					// any key that has a NULL value we can skip placing in
					// the index entirely because NULL values are always <>
					// to NULL values, any key with a NULL value can ALWAYS
					// be placed inside the index
					key_data[i].reset();
					has_null[i] = true;
					break;
				} else {
					// if NULLs are not allowed, throw an exception
					throw ConstraintException(
					    "PRIMARY KEY column cannot contain NULL values!");
				}
			}
			if (TypeIsConstantSize(types[key])) {
				auto data_size = GetTypeIdSize(types[key]);
				memcpy(tuple_data, chunk.data[key].data + data_size * index,
				       data_size);
				tuple_data += data_size;
			} else {
				const char **string_data = (const char **)chunk.data[key].data;
				strcpy(tuple_data, string_data[index]);
				tuple_data += strlen(string_data[index]) + 1;
			}
		}
	}

	UniqueIndexNode *added_nodes[STANDARD_VECTOR_SIZE];

	lock_guard<mutex> guard(index_lock);
	for (size_t i = 0; i < chunk.count; i++) {
		if (has_null[i]) {
			// skip entries with NULL values
			added_nodes[i] = nullptr;
			continue;
		}

		auto entry =
		    AddEntry(key_size[i], move(key_data[i]), row_identifier_start + i);
		if (!entry) {
			// could not add entry: constraint violation
			// clean up previously added keys
			for (size_t j = i; j > 0; j--) {
				if (added_nodes[j - 1]) {
					RemoveEntry(added_nodes[j - 1]);
				}
			}

			// now throw an exception
			throw ConstraintException(
			    "PRIMARY KEY or UNIQUE constraint violated: duplicated key");
		}
		added_nodes[i] = entry;
	}
}
