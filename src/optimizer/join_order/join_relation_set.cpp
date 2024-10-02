#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"

#include <algorithm>

namespace duckdb {

using JoinRelationTreeNode = JoinRelationSetManager::JoinRelationTreeNode;

// LCOV_EXCL_START
string JoinRelationSet::ToString() const {
	string result = "[";
	result += StringUtil::Join(relations, count, ", ", [](const idx_t &relation) { return to_string(relation); });
	result += "]";
	return result;
}
// LCOV_EXCL_STOP

//! Returns true if sub is a subset of super
bool JoinRelationSet::IsSubset(JoinRelationSet &super, JoinRelationSet &sub) {
	D_ASSERT(sub.count > 0);
	if (sub.count > super.count) {
		return false;
	}
	idx_t j = 0;
	for (idx_t i = 0; i < super.count; i++) {
		if (sub.relations[j] == super.relations[i]) {
			j++;
			if (j == sub.count) {
				return true;
			}
		}
	}
	return false;
}

JoinRelationSet &JoinRelationSetManager::GetJoinRelation(unsafe_unique_array<idx_t> relations, idx_t count) {
	// now look it up in the tree
	reference<JoinRelationTreeNode> info(root);
	for (idx_t i = 0; i < count; i++) {
		auto entry = info.get().children.find(relations[i]);
		if (entry == info.get().children.end()) {
			// node not found, create it
			auto insert_it = info.get().children.insert(make_pair(relations[i], make_uniq<JoinRelationTreeNode>()));
			entry = insert_it.first;
		}
		// move to the next node
		info = *entry->second;
	}
	// now check if the JoinRelationSet has already been created
	if (!info.get().relation) {
		// if it hasn't we need to create it
		info.get().relation = make_uniq<JoinRelationSet>(std::move(relations), count);
	}
	return *info.get().relation;
}

//! Create or get a JoinRelationSet from a single node with the given index
JoinRelationSet &JoinRelationSetManager::GetJoinRelation(idx_t index) {
	// create a sorted vector of the relations
	auto relations = make_unsafe_uniq_array<idx_t>(1);
	relations[0] = index;
	idx_t count = 1;
	return GetJoinRelation(std::move(relations), count);
}

JoinRelationSet &JoinRelationSetManager::GetJoinRelation(const unordered_set<idx_t> &bindings) {
	// create a sorted vector of the relations
	unsafe_unique_array<idx_t> relations = bindings.empty() ? nullptr : make_unsafe_uniq_array<idx_t>(bindings.size());
	idx_t count = 0;
	for (auto &entry : bindings) {
		relations[count++] = entry;
	}
	std::sort(relations.get(), relations.get() + count);
	return GetJoinRelation(std::move(relations), count);
}

JoinRelationSet &JoinRelationSetManager::Union(JoinRelationSet &left, JoinRelationSet &right) {
	auto relations = make_unsafe_uniq_array<idx_t>(left.count + right.count);
	idx_t count = 0;
	// move through the left and right relations, eliminating duplicates
	idx_t i = 0, j = 0;
	while (true) {
		if (i == left.count) {
			// exhausted left relation, add remaining of right relation
			for (; j < right.count; j++) {
				relations[count++] = right.relations[j];
			}
			break;
		} else if (j == right.count) {
			// exhausted right relation, add remaining of left
			for (; i < left.count; i++) {
				relations[count++] = left.relations[i];
			}
			break;
		} else if (left.relations[i] < right.relations[j]) {
			// left is smaller, progress left and add it to the set
			relations[count++] = left.relations[i];
			i++;
		} else if (left.relations[i] > right.relations[j]) {
			// right is smaller, progress right and add it to the set
			relations[count++] = right.relations[j];
			j++;
		} else {
			D_ASSERT(left.relations[i] == right.relations[j]);
			relations[count++] = left.relations[i];
			i++;
			j++;
		}
	}
	return GetJoinRelation(std::move(relations), count);
}

// JoinRelationSet *JoinRelationSetManager::Difference(JoinRelationSet *left, JoinRelationSet *right) {
// 	auto relations = unsafe_unique_array<idx_t>(new idx_t[left->count]);
// 	idx_t count = 0;
// 	// move through the left and right relations
// 	idx_t i = 0, j = 0;
// 	while (true) {
// 		if (i == left->count) {
// 			// exhausted left relation, we are done
// 			break;
// 		} else if (j == right->count) {
// 			// exhausted right relation, add remaining of left
// 			for (; i < left->count; i++) {
// 				relations[count++] = left->relations[i];
// 			}
// 			break;
// 		} else if (left->relations[i] == right->relations[j]) {
// 			// equivalent, add nothing
// 			i++;
// 			j++;
// 		} else if (left->relations[i] < right->relations[j]) {
// 			// left is smaller, progress left and add it to the set
// 			relations[count++] = left->relations[i];
// 			i++;
// 		} else {
// 			// right is smaller, progress right
// 			j++;
// 		}
// 	}
// 	return GetJoinRelation(std::move(relations), count);
// }

static string JoinRelationTreeNodeToString(const JoinRelationTreeNode *node) {
	string result = "";
	if (node->relation) {
		result += node->relation.get()->ToString() + "\n";
	}
	for (auto &child : node->children) {
		result += JoinRelationTreeNodeToString(child.second.get());
	}
	return result;
}

string JoinRelationSetManager::ToString() const {
	return JoinRelationTreeNodeToString(&root);
}

void JoinRelationSetManager::Print() {
	Printer::Print(ToString());
}

} // namespace duckdb
