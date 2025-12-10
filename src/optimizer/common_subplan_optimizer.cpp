#include "duckdb/optimizer/common_subplan_optimizer.hpp"

#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/cte_inlining.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/arena_containers/arena_unordered_map.hpp"
#include "duckdb/common/arena_containers/arena_vector.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Subplan Signature/Info
//===--------------------------------------------------------------------===//
enum class ConversionType {
	TO_CANONICAL,
	RESTORE_ORIGINAL,
};

class PlanSignatureColumnIndexMap {
public:
	explicit PlanSignatureColumnIndexMap(ArenaAllocator &allocator)
	    : to_canonical(allocator), restore_original(allocator) {
	}

public:
	template <ConversionType TYPE>
	bool Empty() const {
		return GetMap<TYPE>().empty();
	}

	void Insert(const idx_t original, const idx_t canonical) {
		D_ASSERT(to_canonical.find(original) == to_canonical.end());
		D_ASSERT(restore_original.find(canonical) == restore_original.end());
		to_canonical.emplace(make_pair(original, canonical));
		restore_original.emplace(make_pair(canonical, original));
	}

	template <ConversionType TYPE>
	idx_t Get(const idx_t index) const {
		D_ASSERT(!Empty<TYPE>());
		return GetMap<TYPE>().at(index);
	}

private:
	template <ConversionType TYPE>
	const arena_unordered_map<idx_t, idx_t> &GetMap() const {
		return TYPE == ConversionType::TO_CANONICAL ? to_canonical : restore_original;
	}

private:
	//! Map from original column index to canonical column index (and reverse)
	arena_unordered_map<idx_t, idx_t> to_canonical;
	arena_unordered_map<idx_t, idx_t> restore_original;
};

class PlanSignatureTableIndexMap {
public:
	explicit PlanSignatureTableIndexMap(ArenaAllocator &allocator_p)
	    : allocator(allocator_p), table_index_map(allocator), to_canonical_table_index(allocator),
	      restore_original_table_index(allocator) {
	}

public:
	const arena_unordered_map<idx_t, PlanSignatureColumnIndexMap> &GetMap() const {
		return table_index_map;
	}

	template <ConversionType TYPE>
	bool Convert(LogicalOperator &op) {
		Initialize<TYPE>(op);
		ConvertTableIndices<TYPE>(op);
		ConvertProjectionMaps<TYPE>(op);
		auto can_materialize = ConvertColumnIndices<TYPE>(op);
		ConvertChildren<TYPE>(op);
		return ConvertExpressions<TYPE>(op) && can_materialize;
	}

private:
	template <ConversionType TYPE>
	void Initialize(LogicalOperator &op) {
		if (TYPE == ConversionType::TO_CANONICAL) {
			// Clear temporary data structures
			to_canonical_table_index.clear();
			restore_original_table_index.clear();
			column_ids.clear();
			projection_ids.clear();
			table_indices.clear();
			projection_maps.clear();
			expression_info.clear();

			// Store temporary mapping
			for (const auto &child_op : op.children) {
				for (const auto &child_cb : child_op->GetColumnBindings()) {
					const auto &original = child_cb.table_index;
					auto it = to_canonical_table_index.find(original);
					if (it != to_canonical_table_index.end()) {
						continue; // We've seen this table index before
					}
					const auto canonical = CANONICAL_TABLE_INDEX_OFFSET + to_canonical_table_index.size();
					D_ASSERT(to_canonical_table_index.find(original) == to_canonical_table_index.end());
					D_ASSERT(restore_original_table_index.find(canonical) == restore_original_table_index.end());
					to_canonical_table_index.emplace(make_pair(original, canonical));
					restore_original_table_index.emplace(make_pair(canonical, original));
				}
			}
		}
	}

	template <ConversionType TYPE>
	void ConvertTableIndices(LogicalOperator &op) {
#ifdef D_ASSERT_IS_ENABLED
		const auto table_indices_verification = op.GetTableIndex();
#endif
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
			if (aggr.groupings_index != DConstants::INVALID_INDEX) {
				ConvertTableIndex<TYPE>(aggr.groupings_index, 2);
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_ANY_JOIN:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			auto &join = op.Cast<LogicalJoin>();
			if (join.join_type == JoinType::MARK) {
				ConvertTableIndex<TYPE>(join.mark_index, 0);
			}
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
#ifdef D_ASSERT_IS_ENABLED
		if (TYPE == ConversionType::TO_CANONICAL) {
			D_ASSERT(table_indices == table_indices_verification);
		}
#endif
	}

	template <ConversionType TYPE>
	void ConvertProjectionMaps(LogicalOperator &op) {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_ANY_JOIN:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			auto &join = op.Cast<LogicalJoin>();
			if (TYPE == ConversionType::TO_CANONICAL) {
				D_ASSERT(projection_maps.empty());
				projection_maps.push_back(std::move(join.left_projection_map));
				projection_maps.push_back(std::move(join.right_projection_map));
			} else {
				D_ASSERT(TYPE == ConversionType::RESTORE_ORIGINAL);
				D_ASSERT(!projection_maps.empty());
				join.left_projection_map = std::move(projection_maps[0]);
				join.right_projection_map = std::move(projection_maps[1]);
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_FILTER: {
			auto &filter = op.Cast<LogicalFilter>();
			if (TYPE == ConversionType::TO_CANONICAL) {
				D_ASSERT(projection_maps.empty());
				projection_maps.push_back(std::move(filter.projection_map));
			} else {
				D_ASSERT(TYPE == ConversionType::RESTORE_ORIGINAL);
				D_ASSERT(!projection_maps.empty());
				filter.projection_map = std::move(projection_maps[0]);
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_ORDER_BY: {
			auto &order = op.Cast<LogicalOrder>();
			if (TYPE == ConversionType::TO_CANONICAL) {
				D_ASSERT(projection_maps.empty());
				projection_maps.push_back(std::move(order.projection_map));
			} else {
				D_ASSERT(TYPE == ConversionType::RESTORE_ORIGINAL);
				D_ASSERT(!projection_maps.empty());
				order.projection_map = std::move(projection_maps[0]);
			}
			break;
		}
		default:
			break;
		}
	}

	template <ConversionType TYPE>
	void ConvertTableIndex(idx_t &table_index, const idx_t i) {
		switch (TYPE) {
		case ConversionType::TO_CANONICAL:
			D_ASSERT(table_indices.size() == i);
			D_ASSERT(table_index_map.find(table_index) == table_index_map.end());
			table_index_map.emplace(table_index, PlanSignatureColumnIndexMap(allocator));
			table_indices.emplace_back(table_index);
			table_index = CANONICAL_TABLE_INDEX_OFFSET + to_canonical_table_index.size() + i;
			break;
		case ConversionType::RESTORE_ORIGINAL:
			table_index = table_indices[i];
			break;
		}
	}

	template <ConversionType TYPE>
	bool ConvertColumnIndices(LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op.Cast<LogicalGet>();
			switch (TYPE) {
			case ConversionType::TO_CANONICAL: {
				D_ASSERT(column_ids.empty());
				// Grab selected GET columns and populate with all possible columns
				column_ids = std::move(get.GetMutableColumnIds());
				for (idx_t col_idx = 0; col_idx < get.names.size(); col_idx++) {
					get.GetMutableColumnIds().push_back(ColumnIndex(col_idx));
				}
				for (const auto &vc : get.virtual_columns) {
					get.GetMutableColumnIds().push_back(ColumnIndex(vc.first));
				}

				// Also temporarily don't project any columns out
				projection_ids = std::move(get.projection_ids);

				// FIXME: can probably work for nested columns in the future
				for (const auto &col_id : column_ids) {
					if (col_id.IsVirtualColumn() || col_id.HasChildren()) {
						return false;
					}
				}

				// Store mapping for base tables
				auto &column_index_map = table_index_map.at(table_indices[0]);
				if (projection_ids.empty()) {
					for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
						const auto primary_index = column_ids[col_idx].GetPrimaryIndex();
						column_index_map.Insert(col_idx, primary_index);
					}
				} else {
					for (const auto &proj_id : projection_ids) {
						const auto primary_index = column_ids[proj_id].GetPrimaryIndex();
						column_index_map.Insert(proj_id, primary_index);
					}
				}
				break;
			}
			case ConversionType::RESTORE_ORIGINAL:
				D_ASSERT(!column_ids.empty());
				get.GetMutableColumnIds() = std::move(column_ids);
				D_ASSERT(get.projection_ids.empty());
				get.projection_ids = std::move(projection_ids);
				break;
			}
		}
		return true;
	}

	template <ConversionType TYPE>
	void ConvertChildren(LogicalOperator &op) {
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
	}

	template <ConversionType TYPE>
	bool ConvertExpressions(LogicalOperator &op) {
		const auto &table_index_mapping =
		    TYPE == ConversionType::TO_CANONICAL ? to_canonical_table_index : restore_original_table_index;
		bool can_materialize = true;
		idx_t info_idx = 0;
		LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expr) {
			ExpressionIterator::EnumerateExpression(*expr, [&](unique_ptr<Expression> &child) {
				// Replace column binding
				if (child->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
					auto &column_binding = child->Cast<BoundColumnRefExpression>().binding;
					const auto lookup_idx = TYPE == ConversionType::TO_CANONICAL
					                            ? column_binding.table_index
					                            : restore_original_table_index.at(column_binding.table_index);
					auto &table_map = table_index_map.at(lookup_idx);
					if (!table_map.Empty<TYPE>()) {
						// Replace column index
						column_binding.column_index = table_map.Get<TYPE>(column_binding.column_index);
					}
					// Replace table index
					column_binding.table_index = table_index_mapping.at(column_binding.table_index);
				}

				// Replace default fields
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
	//! Offset for table index conversion
	static constexpr idx_t CANONICAL_TABLE_INDEX_OFFSET = 10000000000000;

	//! For batching allocations
	ArenaAllocator &allocator;

	//! Map from original table index to column index map
	arena_unordered_map<idx_t, PlanSignatureColumnIndexMap> table_index_map;

	//! Temporary map from original table index to canonical table index (and reverse)
	arena_unordered_map<idx_t, idx_t> to_canonical_table_index;
	arena_unordered_map<idx_t, idx_t> restore_original_table_index;
	//! Temporary vector to store table indices
	vector<idx_t> table_indices;
	//! Temporary vector to store projection maps
	vector<vector<idx_t>> projection_maps;

	//! Utility to temporarily store column ids, projection_ids, table indices, expression info and children
	vector<ColumnIndex> column_ids;
	vector<idx_t> projection_ids;
	vector<pair<string, optional_idx>> expression_info;
	vector<unique_ptr<LogicalOperator>> children;
};

class PlanSignatureCreateState {
public:
	explicit PlanSignatureCreateState(ClientContext &context)
	    : stream(DEFAULT_BLOCK_ALLOC_SIZE), serializer(stream), allocator(BufferAllocator::Get(context)),
	      table_index_map(allocator) {
	}

public:
public:
	//! For serializing operators to a binary string
	MemoryStream stream;
	BinarySerializer serializer;

	//! For batching allocations together
	ArenaAllocator allocator;

	//! Manages mappings
	PlanSignatureTableIndexMap table_index_map;
};

class PlanSignature {
public:
	PlanSignature(const MemoryStream &stream_p, idx_t offset_p, idx_t length_p,
	              vector<reference<PlanSignature>> &&child_signatures_p, idx_t operator_count_p,
	              idx_t base_table_count_p, idx_t max_base_table_cardinality_p)
	    : stream(stream_p), offset(offset_p), length(length_p),
	      signature_hash(Hash(stream_p.GetData() + offset, length)), child_signatures(std::move(child_signatures_p)),
	      operator_count(operator_count_p), base_table_count(base_table_count_p),
	      max_base_table_cardinality(max_base_table_cardinality_p) {
	}

public:
	static arena_ptr<PlanSignature> Create(PlanSignatureCreateState &state, LogicalOperator &op,
	                                       vector<reference<PlanSignature>> &&child_signatures) {
		if (!OperatorIsSupported(op)) {
			return nullptr;
		}

		auto can_materialize = state.table_index_map.Convert<ConversionType::TO_CANONICAL>(op);

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
		state.table_index_map.Convert<ConversionType::RESTORE_ORIGINAL>(op);

		if (!can_materialize) {
			return nullptr; // Cannot materialize, no point in adding it
		}

		// Collect some statistics so we can select a good candidate later
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
		return state.allocator.MakePtr<PlanSignature>(state.stream, offset, length, std::move(child_signatures),
		                                              operator_count, base_table_count, max_base_table_cardinality);
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
			// - case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
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

struct Subplan {
	reference<unique_ptr<LogicalOperator>> op;
	arena_vector<ColumnBinding> canonical_bindings;
};

struct SubplanInfo {
	SubplanInfo(ArenaAllocator &allocator, unique_ptr<LogicalOperator> &op,
	            arena_vector<idx_t> &&canonical_table_indices_p, arena_vector<ColumnBinding> &&canonical_bindings)
	    : canonical_table_indices(std::move(canonical_table_indices_p)), subplans(allocator),
	      lowest_common_ancestor(op) {
		subplans.push_back({op, std::move(canonical_bindings)});
	}
	arena_vector<idx_t> canonical_table_indices;
	arena_vector<Subplan> subplans;
	reference<unique_ptr<LogicalOperator>> lowest_common_ancestor;
};

using subplan_map_t = unordered_map<reference<PlanSignature>, SubplanInfo, PlanSignatureHash, PlanSignatureEquality>;

//===--------------------------------------------------------------------===//
// CommonSubplanFinder
//===--------------------------------------------------------------------===//
class CommonSubplanFinder {
public:
	explicit CommonSubplanFinder(ClientContext &context) : state(context) {
	}

private:
	struct OperatorInfo {
		OperatorInfo(unique_ptr<LogicalOperator> &parent_p, const idx_t &depth_p) : parent(parent_p), depth(depth_p) {
		}

		unique_ptr<LogicalOperator> &parent;
		const idx_t depth;
		arena_ptr<PlanSignature> signature;
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
	vector<reference<subplan_map_t::value_type>> FindCommonSubplans(reference<unique_ptr<LogicalOperator>> root) {
		// Find first operator with more than 1 child
		while (root.get()->children.size() == 1) {
			root = root.get()->children[0];
		}

		// Recurse through query plan using stack-based recursion
		arena_vector<StackNode> stack(state.allocator);
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
					const auto current_op_table_index = current.op->GetTableIndex();
					auto it = subplans.find(*signature);
					if (it == subplans.end()) {
						// New subplan, map table indices
						arena_vector<idx_t> canonical_table_indices(state.allocator);
						for (auto &table_index : current_op_table_index) {
							const auto next_canonical_table_index = to_canonical_table_index.size();
							canonical_table_indices.push_back(next_canonical_table_index);
							D_ASSERT(to_canonical_table_index.find(table_index) == to_canonical_table_index.end());
							to_canonical_table_index.emplace(table_index, next_canonical_table_index);
						}

						// Add new subplan
						SubplanInfo subplan_info(state.allocator, current.op, std::move(canonical_table_indices),
						                         GetCanonicalBindings(*current.op));
						subplans.emplace(*signature, std::move(subplan_info));
					} else {
						auto &info = it->second;
						// Matches existing subplan, map already existing table indices
						D_ASSERT(current_op_table_index.size() == info.canonical_table_indices.size());
						for (idx_t i = 0; i < current_op_table_index.size(); i++) {
							to_canonical_table_index.emplace(current_op_table_index[i],
							                                 info.canonical_table_indices[i]);
						}

						// Add subplan to existing
						info.subplans.push_back({current.op, GetCanonicalBindings(*current.op)});
						info.lowest_common_ancestor = LowestCommonAncestor(info.lowest_common_ancestor, current.op);
					}
				}
			}

			// Done with current
			stack.pop_back();
		}

		// Filter out redundant or ineligible subplans before returning
		const auto subplan_infos = GetSortedSubplans();
		vector<subplan_map_t::key_type> to_remove;
		for (auto &entry : subplan_infos) {
			const auto it = subplans.find(entry.get().first);
			D_ASSERT(it != subplans.end());
			const auto &signature = it->first.get();
			auto &subplan_info = it->second;
			if (signature.OperatorCount() == 1) {
				to_remove.push_back(it->first); // Just one operator in this subplan
				continue;
			}
			if (subplan_info.subplans.size() == 1) {
				to_remove.push_back(it->first); // No other identical subplan
				continue;
			}

			auto &parent_op = operator_infos.find(subplan_info.subplans[0].op)->second.parent;
			auto &parent_signature = operator_infos.find(parent_op)->second.signature;
			if (parent_signature) {
				auto parent_it = subplans.find(*parent_signature);
				if (parent_it != subplans.end()) {
					const auto subplan_count = it->second.subplans.size() * signature.OperatorCount();
					const auto parent_count = parent_it->second.subplans.size() * parent_signature->OperatorCount();
					if (parent_count >= subplan_count) {
						to_remove.push_back(it->first); // Parent is better, this one is redundant
						continue;
					}
				}
			}

			// Collect all subplan bindings, and figure out which subplan has the most outgoing bindings
			idx_t max_subplan_idx = 0;
			for (idx_t subplan_idx = 0; subplan_idx < subplan_info.subplans.size(); subplan_idx++) {
				const auto &subplan_bindings = subplan_info.subplans[subplan_idx].canonical_bindings;
				const auto &max_subplan_bindings = subplan_info.subplans[max_subplan_idx].canonical_bindings;
				if (subplan_bindings.size() > max_subplan_bindings.size()) {
					max_subplan_idx = subplan_idx;
				}
			}

			// Move the "maximum subplan" to the front
			std::swap(subplan_info.subplans[0], subplan_info.subplans[max_subplan_idx]);

			// We can bail on a subplan for various reasons (some of which could potentially be fixed)
			bool bail = false;

			// Insert the bindings of the subplan with the most bindings into a set
			column_binding_set_t max_subplan_column_binding_set;
			for (auto &cb : subplan_info.subplans[0].canonical_bindings) {
				if (max_subplan_column_binding_set.find(cb) != max_subplan_column_binding_set.end()) {
					bail = true; // Subplan contains duplicate column bindings, i.e., another nested duplicate subplan
					break;
				}
				max_subplan_column_binding_set.insert(cb);
			}

			// Check if the maximum subplan fully contains the column bindings of the other subplans
			for (idx_t subplan_idx = 1; subplan_idx < subplan_info.subplans.size() && !bail; subplan_idx++) {
				const auto &subplan_bindings = subplan_info.subplans[subplan_idx].canonical_bindings;
				for (auto &cb : subplan_bindings) {
					if (max_subplan_column_binding_set.find(cb) == max_subplan_column_binding_set.end()) {
						bail = true; // Subplan does not fully contain the the other subplans
						break;
					}
				}
			}

			auto &subplan = subplan_info.subplans[0].op.get();
			if (!CTEInlining::EndsInAggregateOrDistinct(*subplan) && !IsSelectiveMultiTablePlan(subplan)) {
				bail = true; // Does not end in agg or distinct, and is not a selective multi table plan
			}

			if (bail) {
				to_remove.push_back(it->first);
			}
		}

		// Only remove them all at the end so the logic above doesn't get affected
		for (auto &signature : to_remove) {
			subplans.erase(signature);
		}

		return GetSortedSubplans();
	}

private:
	arena_ptr<PlanSignature> CreatePlanSignature(const unique_ptr<LogicalOperator> &op) {
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

	arena_vector<ColumnBinding> GetCanonicalBindings(LogicalOperator &op) {
		// Compute the canonical column bindings coming out of this operator for convenience later
		const auto &table_index_map = state.table_index_map.GetMap();
		const auto original_bindings = op.GetColumnBindings();
		arena_vector<ColumnBinding> canonical_bindings(state.allocator);
		for (idx_t col_idx = 0; col_idx < original_bindings.size(); col_idx++) {
			auto &cb = original_bindings[col_idx];
			const auto canonical_table_index = to_canonical_table_index.at(cb.table_index);
			auto &table_map = table_index_map.at(cb.table_index);
			if (table_map.Empty<ConversionType::TO_CANONICAL>()) {
				canonical_bindings.emplace_back(canonical_table_index, cb.column_index);
			} else {
				const auto canonical_col_idx = table_map.Get<ConversionType::TO_CANONICAL>(cb.column_index);
				canonical_bindings.emplace_back(canonical_table_index, canonical_col_idx);
			}
		}
		return canonical_bindings;
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
		static constexpr idx_t CARDINALITY_THRESHOLD = 2048;
		static constexpr idx_t CARDINALITY_RATIO = 2;

		// Must have an estimated cardinality
		if (!op->has_estimated_cardinality) {
			return false;
		}

		// Must select more than 1 base table
		const auto &signature = *operator_infos.find(op)->second.signature;
		if (signature.BaseTableCount() <= 1) {
			return false;
		}

		// If it has this cardinality or less, just materialize
		if (op->estimated_cardinality <= CARDINALITY_THRESHOLD) {
			return true;
		}

		// Otherwise, materialize if it is selective enough
		return op->estimated_cardinality < signature.MaxBaseTableCardinality() / CARDINALITY_RATIO;
	}

	vector<reference<subplan_map_t::value_type>> GetSortedSubplans() {
		// Grab entries from map, and sort by smallest plans first
		vector<reference<subplan_map_t::value_type>> subplan_infos;
		for (auto &entry : subplans) {
			subplan_infos.push_back(entry);
		}
		std::sort(subplan_infos.begin(), subplan_infos.end(),
		          [](reference<subplan_map_t::value_type> lhs, reference<subplan_map_t::value_type> rhs) {
			          return lhs.get().first.get().OperatorCount() < rhs.get().first.get().OperatorCount();
		          });
		return subplan_infos;
	}

private:
	//! State for creating PlanSignature with reusable data structures
	PlanSignatureCreateState state;
	//! Mapping from operator to info
	reference_map_t<unique_ptr<LogicalOperator>, OperatorInfo> operator_infos;
	//! Mapping from subplan signature to subplan information
	subplan_map_t subplans;
	//! Mapping from original table index to canonical table index
	unordered_map<idx_t, idx_t> to_canonical_table_index;
};

//===--------------------------------------------------------------------===//
// CommonSubplanOptimizer
//===--------------------------------------------------------------------===//
CommonSubplanOptimizer::CommonSubplanOptimizer(Optimizer &optimizer_p) : optimizer(optimizer_p) {
}

static idx_t ConvertSubplansToCTE(Optimizer &optimizer, unique_ptr<LogicalOperator> &op, SubplanInfo &subplan_info,
                                  idx_t index, optional_idx min_cte_idx) {
	const auto cte_index = optimizer.binder.GenerateTableIndex();
	const auto cte_name = StringUtil::Format("__common_subplan_%llu", index + 1);

	// Resolve types to be used for creating the materialized CTE and refs
	op->ResolveOperatorTypes();

	// Get types and names
	const auto &primary_subplan = subplan_info.subplans[0];
	const auto &types = primary_subplan.op.get()->types;
	vector<string> col_names;
	for (idx_t i = 0; i < types.size(); i++) {
		col_names.emplace_back(StringUtil::Format("%s_col_%llu", cte_name, i));
	}
	const auto &primary_subplan_bindings = primary_subplan.canonical_bindings;

	// Create CTE refs and figure out column binding replacements
	vector<unique_ptr<LogicalOperator>> cte_refs;
	ColumnBindingReplacer replacer;
	for (idx_t subplan_idx = 0; subplan_idx < subplan_info.subplans.size(); subplan_idx++) {
		const auto &subplan = subplan_info.subplans[subplan_idx];
		const auto cte_ref_index = optimizer.binder.GenerateTableIndex();
		cte_refs.emplace_back(make_uniq<LogicalCTERef>(cte_ref_index, cte_index, types, col_names));
		const auto old_bindings = subplan.op.get()->GetColumnBindings();
		auto new_bindings = cte_refs.back()->GetColumnBindings();
		if (old_bindings.size() != new_bindings.size()) {
			// Different number of output columns - project columns out
			const auto &canonical_bindings = subplan.canonical_bindings;
			vector<unique_ptr<Expression>> select_list;
			for (auto &cb : canonical_bindings) {
				idx_t cte_col_idx = 0;
				for (; cte_col_idx < primary_subplan_bindings.size(); cte_col_idx++) {
					if (cb == primary_subplan_bindings[cte_col_idx]) {
						break;
					}
				}
				D_ASSERT(cte_col_idx < primary_subplan_bindings.size());
				select_list.emplace_back(
				    make_uniq<BoundColumnRefExpression>(types[cte_col_idx], ColumnBinding(cte_ref_index, cte_col_idx)));
			}

			// Place the projection on top
			auto proj = make_uniq<LogicalProjection>(optimizer.binder.GenerateTableIndex(), std::move(select_list));
			proj->children.emplace_back(std::move(cte_refs.back()));
			cte_refs.back() = std::move(proj);
			new_bindings = cte_refs.back()->GetColumnBindings();
		}
		D_ASSERT(old_bindings.size() == new_bindings.size());
		for (idx_t i = 0; i < old_bindings.size(); i++) {
			replacer.replacement_bindings.emplace_back(old_bindings[i], new_bindings[i]);
		}
	}

	// Create the materialized CTE and replace the common subplans with references to it
	auto &lowest_common_ancestor = subplan_info.lowest_common_ancestor.get();
	auto cte =
	    make_uniq<LogicalMaterializedCTE>(cte_name, cte_index, types.size(), std::move(primary_subplan.op.get()),
	                                      std::move(lowest_common_ancestor), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	for (idx_t subplan_idx = 0; subplan_idx < subplan_info.subplans.size(); subplan_idx++) {
		const auto &subplan = subplan_info.subplans[subplan_idx];
		subplan.op.get() = std::move(cte_refs[subplan_idx]);
	}
	lowest_common_ancestor = std::move(cte);

	// Replace bindings of subplans with those of the CTE refs
	replacer.stop_operator = lowest_common_ancestor.get();
	replacer.VisitOperator(*op);                                  // Replace from the root until CTE
	replacer.VisitOperator(*lowest_common_ancestor->children[1]); // Replace in CTE child

	auto &rhs_child = lowest_common_ancestor->children[1];
	if (rhs_child->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		// We have to be careful with the order in which we place the CTEs created by this optimizer
		const auto child_table_index = rhs_child->Cast<LogicalMaterializedCTE>().table_index;
		if (min_cte_idx.IsValid() && child_table_index >= min_cte_idx.GetIndex() && child_table_index < cte_index) {
			// CTEs are in the wrong order - pipeline dependencies cannot be set up, swap previous and current CTE
			auto tmp = std::move(rhs_child->children[1]);
			rhs_child->children[1] = std::move(lowest_common_ancestor);
			lowest_common_ancestor = std::move(rhs_child);
			rhs_child = std::move(tmp);
		}
	}

	return cte_index;
}

unique_ptr<LogicalOperator> CommonSubplanOptimizer::Optimize(unique_ptr<LogicalOperator> op) {
	// Bottom-up identification of identical subplans
	CommonSubplanFinder finder(optimizer.context);
	auto subplans = finder.FindCommonSubplans(op);

	// Convert all subplans to CTEs
	optional_idx min_cte_idx;
	for (idx_t i = 0; i < subplans.size(); i++) {
		auto cte_index = ConvertSubplansToCTE(optimizer, op, subplans[i].get().second, i, min_cte_idx);
		min_cte_idx = min_cte_idx.IsValid() ? min_cte_idx : cte_index;
	}

	return op;
}

} // namespace duckdb
