#include "optimizer/join_order/relation.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

using RelationTreeNode = RelationSetManager::RelationTreeNode;

string RelationSet::ToString() {
	string result = "[";
	for (uint64_t i = 0; i < count; i++) {
		result += std::to_string(relations[i]);
		if (i != count - 1) {
			result += ", ";
		}
	}
	result += "]";
	return result;
}

//! Returns true if sub is a subset of super
bool RelationSet::IsSubset(RelationSet *super, RelationSet *sub) {
	if (sub->count == 0) {
		return false;
	}
	if (sub->count > super->count) {
		return false;
	}
	uint64_t j = 0;
	for (uint64_t i = 0; i < super->count; i++) {
		if (sub->relations[j] == super->relations[i]) {
			j++;
			if (j == sub->count) {
				return true;
			}
		}
	}
	return false;
}

RelationSet *RelationSetManager::GetRelation(unique_ptr<uint64_t[]> relations, uint64_t count) {
	// now look it up in the tree
	RelationTreeNode *info = &root;
	for (uint64_t i = 0; i < count; i++) {
		auto entry = info->children.find(relations[i]);
		if (entry == info->children.end()) {
			// node not found, create it
			auto insert_it = info->children.insert(make_pair(relations[i], make_unique<RelationTreeNode>()));
			entry = insert_it.first;
		}
		// move to the next node
		info = entry->second.get();
	}
	// now check if the RelationSet has already been created
	if (!info->relation) {
		// if it hasn't we need to create it
		info->relation = make_unique<RelationSet>(move(relations), count);
	}
	return info->relation.get();
}

//! Create or get a RelationSet from a single node with the given index
RelationSet *RelationSetManager::GetRelation(uint64_t index) {
	// create a sorted vector of the relations
	auto relations = unique_ptr<uint64_t[]>(new uint64_t[1]);
	relations[0] = index;
	uint64_t count = 1;
	return GetRelation(move(relations), count);
}

RelationSet *RelationSetManager::GetRelation(unordered_set<uint64_t> &bindings) {
	// create a sorted vector of the relations
	unique_ptr<uint64_t[]> relations =
	    bindings.size() == 0 ? nullptr : unique_ptr<uint64_t[]>(new uint64_t[bindings.size()]);
	uint64_t count = 0;
	for (auto &entry : bindings) {
		relations[count++] = entry;
	}
	sort(relations.get(), relations.get() + count);
	return GetRelation(move(relations), count);
}

RelationSet *RelationSetManager::Union(RelationSet *left, RelationSet *right) {
	auto relations = unique_ptr<uint64_t[]>(new uint64_t[left->count + right->count]);
	uint64_t count = 0;
	// move through the left and right relations, eliminating duplicates
	uint64_t i = 0, j = 0;
	while (true) {
		if (i == left->count) {
			// exhausted left relation, add remaining of right relation
			for (; j < right->count; j++) {
				relations[count++] = right->relations[j];
			}
			break;
		} else if (j == right->count) {
			// exhausted right relation, add remaining of left
			for (; i < left->count; i++) {
				relations[count++] = left->relations[i];
			}
			break;
		} else if (left->relations[i] == right->relations[j]) {
			// equivalent, add only one of the two pairs
			relations[count++] = left->relations[i];
			i++;
			j++;
		} else if (left->relations[i] < right->relations[j]) {
			// left is smaller, progress left and add it to the set
			relations[count++] = left->relations[i];
			i++;
		} else {
			// right is smaller, progress right and add it to the set
			relations[count++] = right->relations[j];
			j++;
		}
	}
	return GetRelation(move(relations), count);
}

RelationSet *RelationSetManager::Difference(RelationSet *left, RelationSet *right) {
	auto relations = unique_ptr<uint64_t[]>(new uint64_t[left->count]);
	uint64_t count = 0;
	// move through the left and right relations
	uint64_t i = 0, j = 0;
	while (true) {
		if (i == left->count) {
			// exhausted left relation, we are done
			break;
		} else if (j == right->count) {
			// exhausted right relation, add remaining of left
			for (; i < left->count; i++) {
				relations[count++] = left->relations[i];
			}
			break;
		} else if (left->relations[i] == right->relations[j]) {
			// equivalent, add nothing
			i++;
			j++;
		} else if (left->relations[i] < right->relations[j]) {
			// left is smaller, progress left and add it to the set
			relations[count++] = left->relations[i];
			i++;
		} else {
			// right is smaller, progress right
			j++;
		}
	}
	return GetRelation(move(relations), count);
}
