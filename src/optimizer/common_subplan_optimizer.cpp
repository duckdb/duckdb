#include "duckdb/optimizer/common_subplan_optimizer.hpp"

#include "duckdb/planner/operator/list.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Subplan Signature/Info
//===--------------------------------------------------------------------===//
class PlanSignature {
private:
	PlanSignature() {
	}

public:
	static shared_ptr<PlanSignature> Create(LogicalOperator &op,
	                                        vector<shared_ptr<PlanSignature>> child_signatures = {}) {
		// Construct maps for converting column bindings to uniform representation and back
		static constexpr idx_t UNIFORM_OFFSET = 10000000000000;
		unordered_map<idx_t, idx_t> to_uniform;
		unordered_map<idx_t, idx_t> from_uniform;
		for (const auto &child_op : op.children) {
			for (const auto &child_cb : child_op->GetColumnBindings()) {
				const auto &original = child_cb.table_index;
				auto it = to_uniform.find(original);
				if (it != to_uniform.end()) {
					continue; // We've seen this table index before
				}
				const auto uniform = UNIFORM_OFFSET + to_uniform.size();
				to_uniform[original] = uniform;
				from_uniform[uniform] = original;
			}
		}

		MemoryStream stream;
		BinarySerializer serializer(stream);

		// Convert to uniform table indices
		vector<idx_t> table_indices;
		ConvertTableIndices<true>(op, table_indices);
		ConvertColumnRefs(op, to_uniform);

		// Serialize uniform representation of operator
		serializer.Begin();
		op.Serialize(serializer);
		serializer.End();

		// Convert back table indices
		ConvertTableIndices<false>(op, table_indices);
		ConvertColumnRefs(op, from_uniform);

		auto res = shared_ptr<PlanSignature>(new PlanSignature());
		res->signature.append(char_ptr_cast(stream.GetData()), stream.GetPosition());
		res->children = std::move(child_signatures);
		return res;
	}

	hash_t Hash() const {
		auto res = std::hash<string>()(signature);
		for (auto &child : children) {
			res = CombineHash(res, child->Hash());
		}
		return res;
	}

	bool Equals(const PlanSignature &other) const {
		if (this->signature != other.signature) {
			return false;
		}
		if (this->children.size() != other.children.size()) {
			return false;
		}
		for (idx_t child_idx = 0; child_idx < this->children.size(); ++child_idx) {
			if (!this->children[child_idx]->Equals(*other.children[child_idx])) {
				return false;
			}
		}
		return true;
	}

private:
	template <bool TO_UNIFORM>
	static void ConvertTableIndices(LogicalOperator &op, vector<idx_t> &table_indices) {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_GET: {
			ConvertTableIndicesGeneric<TO_UNIFORM, LogicalGet>(op, table_indices);
			break;
		}
		case LogicalOperatorType::LOGICAL_CHUNK_GET: {
			ConvertTableIndicesGeneric<TO_UNIFORM, LogicalColumnDataGet>(op, table_indices);
			break;
		}
		case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
			ConvertTableIndicesGeneric<TO_UNIFORM, LogicalExpressionGet>(op, table_indices);
			break;
		}
		case LogicalOperatorType::LOGICAL_DUMMY_SCAN: {
			ConvertTableIndicesGeneric<TO_UNIFORM, LogicalDummyScan>(op, table_indices);
			break;
		}
		case LogicalOperatorType::LOGICAL_CTE_REF: {
			ConvertTableIndicesGeneric<TO_UNIFORM, LogicalCTERef>(op, table_indices);
			break;
		}
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			ConvertTableIndicesGeneric<TO_UNIFORM, LogicalProjection>(op, table_indices);
			break;
		}
		case LogicalOperatorType::LOGICAL_PIVOT: {
			auto &pivot = op.Cast<LogicalPivot>();
			if (TO_UNIFORM) {
				table_indices.emplace_back(pivot.pivot_index);
			}
			pivot.pivot_index = TO_UNIFORM ? 0 : table_indices[0];
			break;
		}
		case LogicalOperatorType::LOGICAL_UNNEST: {
			auto &unnest = op.Cast<LogicalUnnest>();
			if (TO_UNIFORM) {
				table_indices.emplace_back(unnest.unnest_index);
			}
			unnest.unnest_index = TO_UNIFORM ? 0 : table_indices[0];
			break;
		}
		case LogicalOperatorType::LOGICAL_WINDOW: {
			auto &window = op.Cast<LogicalWindow>();
			if (TO_UNIFORM) {
				table_indices.emplace_back(window.window_index);
			}
			window.window_index = TO_UNIFORM ? 0 : table_indices[0];
			break;
		}
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto &aggregate = op.Cast<LogicalAggregate>();
			if (TO_UNIFORM) {
				table_indices.emplace_back(aggregate.group_index);
				table_indices.emplace_back(aggregate.aggregate_index);
				table_indices.emplace_back(aggregate.groupings_index);
			}
			aggregate.group_index = TO_UNIFORM ? 0 : table_indices[0];
			aggregate.aggregate_index = TO_UNIFORM ? 1 : table_indices[1];
			aggregate.groupings_index = TO_UNIFORM ? 2 : table_indices[2];
			break;
		}
		case LogicalOperatorType::LOGICAL_UNION:
		case LogicalOperatorType::LOGICAL_EXCEPT:
		case LogicalOperatorType::LOGICAL_INTERSECT: {
			auto &setop = op.Cast<LogicalSetOperation>();
			if (TO_UNIFORM) {
				table_indices.emplace_back(setop.table_index);
			}
			setop.table_index = TO_UNIFORM ? 0 : table_indices[0];
			break;
		}
		default:
			break;
		}
	}

	template <bool TO_UNIFORM, class T>
	static void ConvertTableIndicesGeneric(LogicalOperator &op, vector<idx_t> &table_idxs) {
		auto &generic = op.Cast<T>();
		if (TO_UNIFORM) {
			table_idxs.emplace_back(generic.table_index);
		}
		generic.table_index = TO_UNIFORM ? 0 : table_idxs[0];
	}

	static void ConvertColumnRefs(LogicalOperator &op, const unordered_map<idx_t, idx_t> &table_index_mapping) {
		LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expr) {
			ExpressionIterator::VisitExpressionClassMutable(*expr, ExpressionClass::BOUND_COLUMN_REF,
			                                                [&](unique_ptr<Expression> &child) {
				                                                auto &col_ref = child->Cast<BoundColumnRefExpression>();
				                                                auto &table_index = col_ref.binding.table_index;
				                                                auto it = table_index_mapping.find(table_index);
				                                                D_ASSERT(it != table_index_mapping.end());
				                                                table_index = it->second;
			                                                });
		});
	}

private:
	string signature;
	vector<shared_ptr<PlanSignature>> children;
};

struct PlanSignatureHash {
	std::size_t operator()(const PlanSignature &k) const {
		return k.Hash();
	}
};

struct PlanSignatureEquality {
	bool operator()(const PlanSignature &a, const PlanSignature &b) const {
		return a.Equals(b);
	}
};

struct SubplanInfo {
	explicit SubplanInfo(unique_ptr<LogicalOperator> &op) : subplans({op}), lowest_common_ancestor(op) {
	}
	vector<reference<unique_ptr<LogicalOperator>>> subplans;
	reference<unique_ptr<LogicalOperator>> lowest_common_ancestor;
};

using subplan_map_t = unordered_map<PlanSignature, SubplanInfo, PlanSignatureHash, PlanSignatureEquality>;

//===--------------------------------------------------------------------===//
// PlanSignatureFinder
//===--------------------------------------------------------------------===//
class PlanSignatureFinder {
public:
	PlanSignatureFinder() {
	}

private:
	struct OperatorInfo {
		OperatorInfo(unique_ptr<LogicalOperator> &parent_p, const idx_t &depth_p) : parent(parent_p), depth(depth_p) {
		}

		unique_ptr<LogicalOperator> &parent;
		const idx_t depth;
		shared_ptr<PlanSignature> signature;
	};

	struct StackNode {
		explicit StackNode(unique_ptr<LogicalOperator> &op_p) : op(op_p), child_index(0) {
		}

		bool HasMoreChildren() const {
			return child_index < op->children.size();
		}

		unique_ptr<LogicalOperator> &GetNextChild() {
			D_ASSERT(child_index < op->children.size());
			return op->children[child_index++];
		};

		unique_ptr<LogicalOperator> &op;
		idx_t child_index;
	};

public:
	void FindPlanSignatures(unique_ptr<LogicalOperator> &root) {
		vector<StackNode> stack;
		stack.emplace_back(root);
		operator_infos.emplace(root, OperatorInfo(root, 0));

		while (!stack.empty()) {
			auto &current = stack.back();

			// Depth-first
			if (current.HasMoreChildren()) {
				auto &child = current.GetNextChild();
				operator_infos.emplace(child, OperatorInfo(current.op, stack.size()));
				stack.emplace_back(child);
				continue;
			}

			if (!RefersToSameObject(current.op, root)) {
				// We have all child information for this operator now, compute signature
				auto &signature = operator_infos.find(current.op)->second.signature;
				signature = CreatePlanSignature(current.op);

				// Add to subplans (if we got actually got a signature)
				if (signature) {
					auto it = subplans.find(*signature);
					if (it == subplans.end()) {
						subplans.emplace(*signature, SubplanInfo(current.op));
					} else {
						auto &info = it->second;
						info.subplans.emplace_back(current.op);
						info.lowest_common_ancestor = LowestCommonAncestor(info.lowest_common_ancestor, current.op);
					}
				}
			}

			// Done with current
			stack.pop_back();
		}
	}

private:
	shared_ptr<PlanSignature> CreatePlanSignature(const unique_ptr<LogicalOperator> &op) {
		vector<shared_ptr<PlanSignature>> child_signatures;
		for (auto &child : op->children) {
			auto it = operator_infos.find(child);
			D_ASSERT(it != operator_infos.end());
			if (!it->second.signature) {
				return nullptr; // Failed to create signature from one of the children
			}
			child_signatures.emplace_back(it->second.signature);
		}
		return PlanSignature::Create(*op, std::move(child_signatures));
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
	PlanSignatureFinder finder;
	finder.FindPlanSignatures(op);

	// TODO eliminate common subplans

	return op;
}

} // namespace duckdb
