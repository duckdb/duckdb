#include "duckdb/optimizer/common_subplan_optimizer.hpp"

#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Subplan Signature/Info
//===--------------------------------------------------------------------===//
struct SubplanSignature {
	SubplanSignature() : valid(false) {
	}

	static SubplanSignature CreateFromLeaf(const LogicalOperator &op) {
		D_ASSERT(op.children.empty());
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_GET:
		case LogicalOperatorType::LOGICAL_CHUNK_GET:
		case LogicalOperatorType::LOGICAL_DELIM_GET:
		case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		case LogicalOperatorType::LOGICAL_CTE_REF:
		default:
		}
		throw NotImplementedException("CommonSubplanOptimizer");
	}

	hash_t Hash() const {
		throw NotImplementedException("CommonSubplanOptimizer");
	}

	bool Equals(const SubplanSignature &other) const {
		throw NotImplementedException("CommonSubplanOptimizer");
	}

	hash_t hash;
	bool valid;
};

struct SubplanSignatureHash {
	std::size_t operator()(const SubplanSignature &k) const {
		return k.Hash();
	}
};

struct SubplanSignatureEquality {
	bool operator()(const SubplanSignature &a, const SubplanSignature &b) const {
		return a.Equals(b);
	}
};

struct SubplanInfo {
	explicit SubplanInfo(unique_ptr<LogicalOperator> &op) : subplans({op}), lowest_common_ancestor(op) {
	}
	vector<reference<unique_ptr<LogicalOperator>>> subplans;
	reference<unique_ptr<LogicalOperator>> lowest_common_ancestor;
};

using subplan_map_t = unordered_map<SubplanSignature, SubplanInfo, SubplanSignatureHash, SubplanSignatureEquality>;

//===--------------------------------------------------------------------===//
// CommonSubplanFinder
//===--------------------------------------------------------------------===//
class CommonSubplanFinder {
public:
	CommonSubplanFinder() {
	}

private:
	struct OperatorInfo {
		OperatorInfo(unique_ptr<LogicalOperator> &parent_p, const idx_t &depth_p) : parent(parent_p), depth(depth_p) {
		}

		unique_ptr<LogicalOperator> &parent;
		const idx_t depth;
		SubplanSignature signature;
	};

	struct StackNode {
		explicit StackNode(unique_ptr<LogicalOperator> &op_p) : op(op_p), child_idx(0) {
		}

		bool HasMoreChildren() const {
			return child_idx == op->children.size();
		}

		unique_ptr<LogicalOperator> &GetNextChild() {
			D_ASSERT(child_idx < op->children.size());
			return op->children[child_idx++];
		};

		unique_ptr<LogicalOperator> &op;
		idx_t child_idx;
	};

public:
	void FindCommonSubplans(unique_ptr<LogicalOperator> &root) {
		vector<StackNode> stack;
		stack.emplace_back(root);

		while (!stack.empty()) {
			auto &current = stack.back();

			// Depth-first
			if (current.HasMoreChildren()) {
				auto &child = current.GetNextChild();
				operator_infos.emplace(child, OperatorInfo(current.op, stack.size()));
				stack.emplace_back(current.GetNextChild());
				continue;
			}

			// We have all child information for this operator now, compute signature
			auto &operator_info = operator_infos.find(current.op)->second;
			operator_info.signature = GetSubplanSignature(current.op);

			// Add to subplans
			auto it = subplans.find(operator_info.signature);
			if (it == subplans.end()) {
				subplans.emplace(operator_info.signature, SubplanInfo(current.op));
			} else {
				auto &info = it->second;
				info.subplans.emplace_back(current.op);
				info.lowest_common_ancestor = LowestCommonAncestor(info.lowest_common_ancestor, current.op);
			}

			// Done with current
			stack.pop_back();
		}
	}

private:
	SubplanSignature GetSubplanSignature(const LogicalOperator &op) {
		if (op.children.empty()) {
			return SubplanSignature::CreateFromLeaf(op);
		}
		throw NotImplementedException("CommonSubplanOptimizer");
	}

	unique_ptr<LogicalOperator> &LowestCommonAncestor(reference<unique_ptr<LogicalOperator>> a,
	                                                  reference<unique_ptr<LogicalOperator>> b) {
		auto a_it = operator_infos.find(a);
		auto b_it = operator_infos.find(b);
		D_ASSERT(a_it != operator_infos.end() && b_it != operator_infos.end());

		// Get parents of a and b until they're at the same depth
		while (a_it->second.depth > b_it->second.depth) {
			a = a_it->second.parent;
			a_it = operator_infos.find(a);
			D_ASSERT(a_it != operator_infos.end());
		}
		while (b_it->second.depth > a_it->second.depth) {
			b = b_it->second.parent;
			b_it = operator_infos.find(b);
			D_ASSERT(b_it != operator_infos.end());
		}

		// Move up one level at a time for both until ancestor is the same
		while (!RefersToSameObject(a, b)) {
			a_it = operator_infos.find(a);
			b_it = operator_infos.find(b);
			D_ASSERT(a_it != operator_infos.end() && b_it != operator_infos.end());
			a = a_it->second.parent;
			b = b_it->second.parent;
		}

		return a.get();
	}

private:
	//! Mapping from operator to info
	reference_map_t<unique_ptr<LogicalOperator>, OperatorInfo> operator_infos;
	//! Mapping from subplan signature to subplan information
	subplan_map_t subplans;
};

//===--------------------------------------------------------------------===//
// CommonSubplanOptimizer
//===--------------------------------------------------------------------===//
CommonSubplanOptimizer::CommonSubplanOptimizer(Optimizer &optimizer_p) : optimizer(optimizer_p) {
}

unique_ptr<LogicalOperator> CommonSubplanOptimizer::Optimize(unique_ptr<LogicalOperator> op) {
	CommonSubplanFinder finder;
	finder.FindCommonSubplans(op);

	// TODO eliminate common subplans

	return op;
}

} // namespace duckdb
