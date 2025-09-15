#include "duckdb/optimizer/common_subplan_optimizer.hpp"

#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Subplan Signature/Info
//===--------------------------------------------------------------------===//
class PlanSignature {
private:
	explicit PlanSignature(const idx_t operator_count_p) : operator_count(operator_count_p) {
	}

public:
	static unique_ptr<PlanSignature> Create(LogicalOperator &op, vector<reference<PlanSignature>> child_signatures,
	                                        const idx_t operator_count) {
		if (!OperatorIsSupported(op)) {
			return nullptr;
		}

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

		auto res = unique_ptr<PlanSignature>(new PlanSignature(operator_count));
		res->signature.append(char_ptr_cast(stream.GetData()), stream.GetPosition());
		res->children = std::move(child_signatures);
		return res;
	}

	idx_t OperatorCount() const {
		return operator_count;
	}

	hash_t Hash() const {
		auto res = std::hash<string>()(signature);
		for (auto &child : children) {
			res = CombineHash(res, child.get().Hash());
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
			if (!this->children[child_idx].get().Equals(other.children[child_idx].get())) {
				return false;
			}
		}
		return true;
	}

private:
	static bool OperatorIsSupported(const LogicalOperator &op) {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_PROJECTION:
		case LogicalOperatorType::LOGICAL_FILTER:
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		case LogicalOperatorType::LOGICAL_WINDOW:
		case LogicalOperatorType::LOGICAL_UNNEST:
		case LogicalOperatorType::LOGICAL_LIMIT:
		case LogicalOperatorType::LOGICAL_ORDER_BY:
		case LogicalOperatorType::LOGICAL_TOP_N:
		case LogicalOperatorType::LOGICAL_DISTINCT:
		case LogicalOperatorType::LOGICAL_PIVOT:
		case LogicalOperatorType::LOGICAL_GET:
		case LogicalOperatorType::LOGICAL_CHUNK_GET:
		case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		case LogicalOperatorType::LOGICAL_CTE_REF:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		case LogicalOperatorType::LOGICAL_ANY_JOIN:
		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
		case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		case LogicalOperatorType::LOGICAL_UNION:
		case LogicalOperatorType::LOGICAL_EXCEPT:
		case LogicalOperatorType::LOGICAL_INTERSECT:
			return true;
		default:
			// Unsupported:
			// - case LogicalOperatorType::LOGICAL_COPY_TO_FILE:
			// - case LogicalOperatorType::LOGICAL_SAMPLE:
			// - case LogicalOperatorType::LOGICAL_COPY_DATABASE:
			// - case LogicalOperatorType::LOGICAL_DELIM_GET:
			// - case LogicalOperatorType::LOGICAL_JOIN:
			// - case LogicalOperatorType::LOGICAL_DELIM_JOIN:
			// - case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
			// - case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
			// - case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
			// - case LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR
			return false;
		}
	}

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
	vector<reference<PlanSignature>> children;
	const idx_t operator_count;
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
		unique_ptr<PlanSignature> signature;
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
	subplan_map_t FindCommonSubplans(unique_ptr<LogicalOperator> &root) {
		// Recurse through query plan using stack-based recursion
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

		// Remove redundant subplans before returning
		for (auto it = subplans.begin(); it != subplans.end();) {
			if (it->first.OperatorCount() == 1) {
				it = subplans.erase(it); // Just one operator in this subplan
				continue;
			}
			if (it->second.subplans.size() == 1) {
				it = subplans.erase(it); // No other identical subplan
				continue;
			}
			auto &parent_signature = *operator_infos.find(it->second.subplans[0].get())->second.signature;
			auto parent_it = subplans.find(parent_signature);
			if (parent_it != subplans.end() && it->second.subplans.size() == parent_it->second.subplans.size()) {
				it = subplans.erase(it); // Parent has exact same number of identical subplans
				continue;
			}
			it++; // This subplan is not redundant
		}

		return std::move(subplans);
	}

private:
	unique_ptr<PlanSignature> CreatePlanSignature(const unique_ptr<LogicalOperator> &op) {
		vector<reference<PlanSignature>> child_signatures;
		idx_t operator_count = 1;
		for (auto &child : op->children) {
			auto it = operator_infos.find(child);
			D_ASSERT(it != operator_infos.end());
			if (!it->second.signature) {
				return nullptr; // Failed to create signature from one of the children
			}
			child_signatures.emplace_back(*it->second.signature);
			operator_count += it->second.signature->OperatorCount();
		}
		return PlanSignature::Create(*op, std::move(child_signatures), operator_count);
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

static unique_ptr<LogicalOperator> ConvertSubplansToCTE(Optimizer &optimizer, unique_ptr<LogicalOperator> op,
                                                        SubplanInfo &subplan_info) {
	auto &subplan = subplan_info.subplans[0].get();
	auto cte = make_uniq<LogicalMaterializedCTE>("todo", optimizer.binder.GenerateTableIndex(),
	                                             subplan->GetColumnBindings().size(), nullptr, nullptr, // TODO
	                                             CTEMaterialize::CTE_MATERIALIZE_DEFAULT);

	return op;
}

unique_ptr<LogicalOperator> CommonSubplanOptimizer::Optimize(unique_ptr<LogicalOperator> op) {
	// Bottom-up identification of identical subplans
	CommonSubplanFinder finder;
	auto subplans = finder.FindCommonSubplans(op);

	// Identify the single best subplan (TODO: for now, in the future we should identify multiple)
	if (subplans.empty()) {
		return op; // No matching subplans
	}
	auto best_it = subplans.begin();
	for (auto it = ++subplans.begin(); it != subplans.end(); it++) {
		if (it->first.OperatorCount() > best_it->first.OperatorCount()) {
			best_it = it;
		}
	}

	// Create a CTE!
	return ConvertSubplansToCTE(optimizer, std::move(op), best_it->second);
}

} // namespace duckdb
