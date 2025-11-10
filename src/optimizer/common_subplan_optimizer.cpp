#include "duckdb/optimizer/common_subplan_optimizer.hpp"

#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/cte_inlining.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Subplan Signature/Info
//===--------------------------------------------------------------------===//
enum class ConversionType {
	TO_CANONICAL,
	RESTORE_ORIGINAL,
};

class PlanSignatureCreateState {
public:
	PlanSignatureCreateState() : stream(DEFAULT_BLOCK_ALLOC_SIZE), serializer(stream) {
	}

public:
	void Initialize(LogicalOperator &op) {
		to_canonical.clear();
		from_canonical.clear();
		table_indices.clear();
		expression_info.clear();

		for (const auto &child_op : op.children) {
			for (const auto &child_cb : child_op->GetColumnBindings()) {
				const auto &original = child_cb.table_index;
				auto it = to_canonical.find(original);
				if (it != to_canonical.end()) {
					continue; // We've seen this table index before
				}
				const auto canonical = CANONICAL_TABLE_INDEX_OFFSET + to_canonical.size();
				to_canonical[original] = canonical;
				from_canonical[canonical] = original;
			}
		}
	}

	template <ConversionType TYPE>
	bool Convert(LogicalOperator &op) {
		switch (TYPE) {
		case ConversionType::TO_CANONICAL:
			D_ASSERT(children.empty());
			children = std::move(op.children);
			break;
		case ConversionType::RESTORE_ORIGINAL:
			D_ASSERT(op.children.empty());
			op.children = std::move(children);
			break;
		}
		ConvertTableIndices<TYPE>(op);
		return ConvertExpressions<TYPE>(op);
	}

private:
	template <ConversionType TYPE>
	void ConvertTableIndices(LogicalOperator &op) {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_GET:
			ConvertTableIndex<TYPE>(op.Cast<LogicalGet>().table_index, 0);
			break;
		case LogicalOperatorType::LOGICAL_CHUNK_GET:
			ConvertTableIndex<TYPE>(op.Cast<LogicalColumnDataGet>().table_index, 0);
			break;
		case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
			ConvertTableIndex<TYPE>(op.Cast<LogicalExpressionGet>().table_index, 0);
			break;
		case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
			ConvertTableIndex<TYPE>(op.Cast<LogicalDummyScan>().table_index, 0);
			break;
		case LogicalOperatorType::LOGICAL_CTE_REF:
			ConvertTableIndex<TYPE>(op.Cast<LogicalCTERef>().table_index, 0);
			break;
		case LogicalOperatorType::LOGICAL_PROJECTION:
			ConvertTableIndex<TYPE>(op.Cast<LogicalProjection>().table_index, 0);
			break;
		case LogicalOperatorType::LOGICAL_PIVOT:
			ConvertTableIndex<TYPE>(op.Cast<LogicalPivot>().pivot_index, 0);
			break;
		case LogicalOperatorType::LOGICAL_UNNEST:
			ConvertTableIndex<TYPE>(op.Cast<LogicalUnnest>().unnest_index, 0);
			break;
		case LogicalOperatorType::LOGICAL_WINDOW:
			ConvertTableIndex<TYPE>(op.Cast<LogicalWindow>().window_index, 0);
			break;
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto &aggr = op.Cast<LogicalAggregate>();
			ConvertTableIndex<TYPE>(aggr.group_index, 0);
			ConvertTableIndex<TYPE>(aggr.aggregate_index, 1);
			ConvertTableIndex<TYPE>(aggr.groupings_index, 2);
			break;
		}
		case LogicalOperatorType::LOGICAL_UNION:
		case LogicalOperatorType::LOGICAL_EXCEPT:
		case LogicalOperatorType::LOGICAL_INTERSECT:
			ConvertTableIndex<TYPE>(op.Cast<LogicalSetOperation>().table_index, 0);
			break;
		default:
			break;
		}
	}

	template <ConversionType TYPE>
	void ConvertTableIndex(idx_t &table_index, const idx_t i) {
		switch (TYPE) {
		case ConversionType::TO_CANONICAL:
			D_ASSERT(table_indices.size() == i);
			table_indices.emplace_back(table_index);
			table_index = CANONICAL_TABLE_INDEX_OFFSET + i;
			break;
		case ConversionType::RESTORE_ORIGINAL:
			table_index = table_indices[i];
			break;
		}
	}

	template <ConversionType TYPE>
	bool ConvertExpressions(LogicalOperator &op) {
		const auto &table_index_mapping = TYPE == ConversionType::TO_CANONICAL ? to_canonical : from_canonical;
		bool can_materialize = true;
		idx_t info_idx = 0;
		LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expr) {
			ExpressionIterator::EnumerateExpression(*expr, [&](unique_ptr<Expression> &child) {
				if (child->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
					auto &col_ref = child->Cast<BoundColumnRefExpression>();
					auto &table_index = col_ref.binding.table_index;
					auto it = table_index_mapping.find(table_index);
					D_ASSERT(it != table_index_mapping.end());
					table_index = it->second;
				}
				switch (TYPE) {
				case ConversionType::TO_CANONICAL:
					expression_info.emplace_back(std::move(child->alias), child->query_location);
					child->alias.clear();
					child->query_location.SetInvalid();
					break;
				case ConversionType::RESTORE_ORIGINAL:
					auto &info = expression_info[info_idx++];
					child->alias = std::move(info.first);
					child->query_location = info.second;
					break;
				}
				if (child->IsVolatile()) {
					can_materialize = false;
				}
			});
		});
		return can_materialize;
	}

private:
	static constexpr idx_t CANONICAL_TABLE_INDEX_OFFSET = 10000000000000;

public:
	MemoryStream stream;
	BinarySerializer serializer;

	//! Mapping from original table index to canonical table index (and reverse mapping)
	unordered_map<idx_t, idx_t> to_canonical;
	unordered_map<idx_t, idx_t> from_canonical;

	//! Place to temporarily store children
	vector<unique_ptr<LogicalOperator>> children;

	//! Utility vectors to temporarily store table indices and expression info
	vector<idx_t> table_indices;
	vector<pair<string, optional_idx>> expression_info;
};

class PlanSignature {
private:
	PlanSignature(const MemoryStream &stream_p, idx_t offset_p, idx_t length_p,
	              vector<reference<PlanSignature>> &&child_signatures_p, idx_t operator_count_p,
	              idx_t base_table_count_p, idx_t max_base_table_cardinality_p)
	    : stream(stream_p), offset(offset_p), length(length_p),
	      signature_hash(Hash(stream_p.GetData() + offset, length)), child_signatures(std::move(child_signatures_p)),
	      operator_count(operator_count_p), base_table_count(base_table_count_p),
	      max_base_table_cardinality(max_base_table_cardinality_p) {
	}

public:
	static unique_ptr<PlanSignature> Create(PlanSignatureCreateState &state, LogicalOperator &op,
	                                        vector<reference<PlanSignature>> &&child_signatures) {
		if (!OperatorIsSupported(op)) {
			return nullptr;
		}
		state.Initialize(op);

		auto can_materialize = state.Convert<ConversionType::TO_CANONICAL>(op);

		// Serialize canonical representation of operator
		const auto offset = state.stream.GetPosition();
		state.serializer.Begin();
		try { // Operators will throw if they cannot serialize, so we need to try/catch here
			op.Serialize(state.serializer);
		} catch (std::exception &) {
			can_materialize = false;
		}
		state.serializer.End();
		const auto length = state.stream.GetPosition() - offset;

		// Convert back from canonical
		state.Convert<ConversionType::RESTORE_ORIGINAL>(op);

		if (can_materialize) {
			idx_t operator_count = 1;
			idx_t base_table_count = 0;
			idx_t max_base_table_cardinality = 0;
			if (op.children.empty()) {
				base_table_count++;
				if (op.has_estimated_cardinality) {
					max_base_table_cardinality = op.estimated_cardinality;
				}
			}
			for (auto &child_signature : child_signatures) {
				operator_count += child_signature.get().OperatorCount();
				base_table_count += child_signature.get().BaseTableCount();
				max_base_table_cardinality =
				    MaxValue(max_base_table_cardinality, child_signature.get().MaxBaseTableCardinality());
			}
			return unique_ptr<PlanSignature>(new PlanSignature(state.stream, offset, length,
			                                                   std::move(child_signatures), operator_count,
			                                                   base_table_count, max_base_table_cardinality));
		}
		return nullptr;
	}

	idx_t OperatorCount() const {
		return operator_count;
	}

	idx_t BaseTableCount() const {
		return base_table_count;
	}

	idx_t MaxBaseTableCardinality() const {
		return max_base_table_cardinality;
	}

	hash_t HashSignature() const {
		auto res = signature_hash;
		for (auto &child : child_signatures) {
			res = CombineHash(res, child.get().HashSignature());
		}
		return res;
	}

	bool Equals(const PlanSignature &other) const {
		if (this->GetSignature() != other.GetSignature()) {
			return false;
		}
		if (this->child_signatures.size() != other.child_signatures.size()) {
			return false;
		}
		for (idx_t child_idx = 0; child_idx < this->child_signatures.size(); ++child_idx) {
			if (!this->child_signatures[child_idx].get().Equals(other.child_signatures[child_idx].get())) {
				return false;
			}
		}
		return true;
	}

private:
	String GetSignature() const {
		return String(char_ptr_cast(stream.GetData() + offset), NumericCast<uint32_t>(length));
	}

	static bool OperatorIsSupported(const LogicalOperator &op) {
		if (!op.SupportSerialization()) {
			return false;
		}
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
		case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		case LogicalOperatorType::LOGICAL_ANY_JOIN:
		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
		case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		case LogicalOperatorType::LOGICAL_UNION:
		case LogicalOperatorType::LOGICAL_EXCEPT:
		case LogicalOperatorType::LOGICAL_INTERSECT:
			return true;
		case LogicalOperatorType::LOGICAL_CHUNK_GET:
			// Avoid serializing massive amounts of data (this is here because of the "Test TPCH arrow roundtrip" test)
			return op.Cast<LogicalColumnDataGet>().collection->Count() < 1000;
		default:
			// Unsupported:
			// - case LogicalOperatorType::LOGICAL_COPY_TO_FILE:
			// - case LogicalOperatorType::LOGICAL_SAMPLE:
			// - case LogicalOperatorType::LOGICAL_COPY_DATABASE:
			// - case LogicalOperatorType::LOGICAL_DELIM_GET:
			// - case LogicalOperatorType::LOGICAL_CTE_REF:
			// - case LogicalOperatorType::LOGICAL_JOIN:
			// - case LogicalOperatorType::LOGICAL_DELIM_JOIN:
			// - case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
			// - case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
			// - case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
			// - case LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR
			return false;
		}
	}

private:
	const MemoryStream &stream;
	const idx_t offset;
	const idx_t length;

	const hash_t signature_hash;

	const vector<reference<PlanSignature>> child_signatures;
	const idx_t operator_count;
	const idx_t base_table_count;
	const idx_t max_base_table_cardinality;
};

struct PlanSignatureHash {
	std::size_t operator()(const PlanSignature &k) const {
		return k.HashSignature();
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

using subplan_map_t = unordered_map<reference<PlanSignature>, SubplanInfo, PlanSignatureHash, PlanSignatureEquality>;

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
	subplan_map_t FindCommonSubplans(reference<unique_ptr<LogicalOperator>> root) {
		// Find first operator with more than 1 child
		while (root.get()->children.size() == 1) {
			root = root.get()->children[0];
		}

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

			if (!RefersToSameObject(current.op, root.get())) {
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

		// Filter out redundant or ineligible subplans before returning
		for (auto it = subplans.begin(); it != subplans.end();) {
			if (it->first.get().OperatorCount() == 1) {
				it = subplans.erase(it); // Just one operator in this subplan
				continue;
			}
			if (it->second.subplans.size() == 1) {
				it = subplans.erase(it); // No other identical subplan
				continue;
			}
			auto &subplan = it->second.subplans[0].get();
			auto &parent = operator_infos.find(subplan)->second.parent;
			auto &parent_signature = operator_infos.find(parent)->second.signature;
			if (parent_signature) {
				auto parent_it = subplans.find(*parent_signature);
				if (parent_it != subplans.end() && it->second.subplans.size() == parent_it->second.subplans.size()) {
					it = subplans.erase(it); // Parent has exact same number of identical subplans
					continue;
				}
			}
			if (CTEInlining::EndsInAggregateOrDistinct(*subplan) || IsSelectiveMultiTablePlan(subplan)) {
				it++; // This subplan might be useful
			} else {
				it = subplans.erase(it); // Not eligible for materialization
			}
		}

		return std::move(subplans);
	}

private:
	unique_ptr<PlanSignature> CreatePlanSignature(const unique_ptr<LogicalOperator> &op) {
		vector<reference<PlanSignature>> child_signatures;
		for (auto &child : op->children) {
			auto it = operator_infos.find(child);
			D_ASSERT(it != operator_infos.end());
			if (!it->second.signature) {
				return nullptr; // Failed to create signature from one of the children
			}
			child_signatures.emplace_back(*it->second.signature);
		}
		return PlanSignature::Create(state, *op, std::move(child_signatures));
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

	bool IsSelectiveMultiTablePlan(unique_ptr<LogicalOperator> &op) const {
		static constexpr idx_t CARDINALITY_RATIO = 2;

		if (!op->has_estimated_cardinality) {
			return false;
		}
		const auto &signature = *operator_infos.find(op)->second.signature;
		if (signature.BaseTableCount() <= 1) {
			return false;
		}
		return op->estimated_cardinality < signature.MaxBaseTableCardinality() / CARDINALITY_RATIO;
	}

private:
	//! Mapping from operator to info
	reference_map_t<unique_ptr<LogicalOperator>, OperatorInfo> operator_infos;
	//! Mapping from subplan signature to subplan information
	subplan_map_t subplans;
	//! State for creating PlanSignature with reusable data structures
	PlanSignatureCreateState state;
};

//===--------------------------------------------------------------------===//
// CommonSubplanOptimizer
//===--------------------------------------------------------------------===//
CommonSubplanOptimizer::CommonSubplanOptimizer(Optimizer &optimizer_p) : optimizer(optimizer_p) {
}

static void ConvertSubplansToCTE(Optimizer &optimizer, unique_ptr<LogicalOperator> &op, SubplanInfo &subplan_info) {
	const auto cte_index = optimizer.binder.GenerateTableIndex();
	const auto cte_name = StringUtil::Format("__common_subplan_1");

	// Resolve types to be used for creating the materialized CTE and refs
	op->ResolveOperatorTypes();

	// Get types and names
	const auto &types = subplan_info.subplans[0].get()->types;
	vector<string> col_names;
	for (idx_t i = 0; i < types.size(); i++) {
		col_names.emplace_back(StringUtil::Format("%s_col_%llu", cte_name, i));
	}

	// Create CTE refs and figure out column binding replacements
	vector<unique_ptr<LogicalCTERef>> cte_refs;
	ColumnBindingReplacer replacer;
	for (auto &subplan : subplan_info.subplans) {
		cte_refs.emplace_back(
		    make_uniq<LogicalCTERef>(optimizer.binder.GenerateTableIndex(), cte_index, types, col_names));
		const auto old_bindings = subplan.get()->GetColumnBindings();
		const auto new_bindings = cte_refs.back()->GetColumnBindings();
		D_ASSERT(old_bindings.size() == new_bindings.size());
		for (idx_t i = 0; i < old_bindings.size(); i++) {
			replacer.replacement_bindings.emplace_back(old_bindings[i], new_bindings[i]);
		}
	}

	// Create the materialized CTE and replace the common subplans with references to it
	auto &lowest_common_ancestor = subplan_info.lowest_common_ancestor.get();
	auto cte =
	    make_uniq<LogicalMaterializedCTE>(cte_name, cte_index, types.size(), std::move(subplan_info.subplans[0].get()),
	                                      std::move(lowest_common_ancestor), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	for (idx_t i = 0; i < subplan_info.subplans.size(); i++) {
		subplan_info.subplans[i].get() = std::move(cte_refs[i]);
	}
	lowest_common_ancestor = std::move(cte);

	// Replace bindings of subplans with those of the CTE refs
	replacer.stop_operator = lowest_common_ancestor.get();
	replacer.VisitOperator(*op);                                  // Replace from the root until CTE
	replacer.VisitOperator(*lowest_common_ancestor->children[1]); // Replace in CTE child
}

unique_ptr<LogicalOperator> CommonSubplanOptimizer::Optimize(unique_ptr<LogicalOperator> op) {
	// Bottom-up identification of identical subplans
	CommonSubplanFinder finder;
	auto subplans = finder.FindCommonSubplans(op);

	// Identify the single best subplan (TODO: for now, in the future we should identify multiple)
	if (subplans.empty()) {
		return op; // No eligible subplans
	}
	auto best_it = subplans.begin();
	for (auto it = ++subplans.begin(); it != subplans.end(); it++) {
		if (it->first.get().OperatorCount() > best_it->first.get().OperatorCount()) {
			best_it = it;
		}
	}

	// Create a CTE!
	ConvertSubplansToCTE(optimizer, op, best_it->second);
	return op;
}

} // namespace duckdb
