#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/optimizer/in_clause_rewriter.hpp"

namespace duckdb {

namespace {

//! The constant values of an IN, probed with the same equality semantics as VectorOperations::Equals
class InValueSet {
public:
	virtual ~InValueSet() = default;

	virtual void Add(const Vector &constant) = 0;
	virtual void Finalize() = 0;

	void Probe(const Vector &input, idx_t count, bool negate, Vector &result) const {
		if (input.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
			// probe each dictionary entry only once
			auto dictionary_size = DictionaryVector::DictionarySize(input);
			if (dictionary_size.IsValid() && dictionary_size.GetIndex() < count) {
				Vector dictionary_result(LogicalType::BOOLEAN, dictionary_size.GetIndex());
				ProbeValues(DictionaryVector::Child(input), dictionary_size.GetIndex(), negate, dictionary_result);
				result.Slice(dictionary_result, DictionaryVector::SelVector(input), count);
				return;
			}
		}
		ProbeValues(input, count, negate, result);
	}

protected:
	virtual void ProbeValues(const Vector &input, idx_t count, bool negate, Vector &result) const = 0;

protected:
	bool has_null = false;
};

template <class T>
class TemplatedInValueSet : public InValueSet {
	//! Integer values spanning at most this many slots (or 32 per value) are stored in a bitmap
	static constexpr uint64_t MAX_BITMAP_RANGE = 1ULL << 16;
	static constexpr bool SUPPORTS_BITMAP = std::is_integral<T>::value && !std::is_same<T, bool>::value;
	//! Hash table slots are kept sparse for up to this many values, so misses mostly hit an empty slot
	static constexpr idx_t MAX_SPARSE_TABLE_VALUES = 1ULL << 16;
	//! Rows that are hashed in one pass before probing
	static constexpr idx_t PROBE_BATCH_SIZE = 256;

public:
	void Add(const Vector &constant) override {
		auto entry = constant.Values<T>()[0];
		if (!entry.IsValid()) {
			has_null = true;
			return;
		}
		collected.push_back(StoreValue(entry.GetValue()));
	}

	void Finalize() override {
		if (!TryBuildBitmap()) {
			BuildTable();
		}
		collected.clear();
	}

protected:
	void ProbeValues(const Vector &input, idx_t count, bool negate, Vector &result) const override {
		auto input_values = input.Values<T>();
		auto writer = FlatVector::Writer<bool>(result, count);
		if (!bitmap.empty()) {
			for (idx_t i = 0; i < count; i++) {
				auto entry = input_values[i];
				WriteResult(writer, entry.IsValid(), entry.IsValid() && BitmapContains(entry.GetValueUnsafe()), negate);
			}
			return;
		}
		hash_t hashes[PROBE_BATCH_SIZE];
		for (idx_t base = 0; base < count; base += PROBE_BATCH_SIZE) {
			auto batch_size = MinValue<idx_t>(count - base, PROBE_BATCH_SIZE);
			for (idx_t i = 0; i < batch_size; i++) {
				auto entry = input_values[base + i];
				hashes[i] = entry.IsValid() ? Hash<T>(entry.GetValueUnsafe()) : 0;
			}
			for (idx_t i = 0; i < batch_size; i++) {
				auto entry = input_values[base + i];
				WriteResult(writer, entry.IsValid(),
				            entry.IsValid() && TableContains(entry.GetValueUnsafe(), hashes[i]), negate);
			}
		}
	}

private:
	T StoreValue(const T &value) {
		return value;
	}

	void WriteResult(VectorWriter<bool> &writer, bool is_valid, bool found, bool negate) const {
		if (!is_valid || (!found && has_null)) {
			writer.WriteNull();
		} else {
			writer.WriteValue(found != negate);
		}
	}

	bool TryBuildBitmap() {
		if constexpr (SUPPORTS_BITMAP) {
			if (collected.empty()) {
				return false;
			}
			T min_value = collected[0];
			T max_value = collected[0];
			for (const T value : collected) {
				min_value = MinValue(min_value, value);
				max_value = MaxValue(max_value, value);
			}
			// two's complement subtraction yields the exact range, also for signed types
			auto range = static_cast<uint64_t>(max_value) - static_cast<uint64_t>(min_value);
			if (range >= MaxValue<uint64_t>(MAX_BITMAP_RANGE, collected.size() * 32)) {
				return false;
			}
			bitmap_min = static_cast<uint64_t>(min_value);
			bitmap_range = range;
			bitmap.resize(range / 64 + 1, 0);
			for (const T value : collected) {
				auto offset = static_cast<uint64_t>(value) - bitmap_min;
				bitmap[offset / 64] |= 1ULL << (offset % 64);
			}
			return true;
		} else {
			return false;
		}
	}

	bool BitmapContains(const T &value) const {
		if constexpr (SUPPORTS_BITMAP) {
			auto offset = static_cast<uint64_t>(value) - bitmap_min;
			return offset <= bitmap_range && (bitmap[offset / 64] >> (offset % 64)) & 1;
		} else {
			return false;
		}
	}

	//! Slot tags are the hash with the lowest bit set, a zero tag marks an empty slot
	static hash_t SlotTag(hash_t hash) {
		return hash | 1;
	}

	void BuildTable() {
		auto load_factor = collected.size() <= MAX_SPARSE_TABLE_VALUES ? 4 : 2;
		auto capacity = NextPowerOfTwo(MaxValue<idx_t>(collected.size() * load_factor, 16));
		slot_tags.resize(capacity, 0);
		slot_values.resize(capacity);
		mask = capacity - 1;
		for (const T value : collected) {
			auto hash = Hash<T>(value);
			if (TableContains(value, hash)) {
				continue;
			}
			auto idx = hash & mask;
			while (slot_tags[idx] != 0) {
				idx = (idx + 1) & mask;
			}
			slot_tags[idx] = SlotTag(hash);
			slot_values[idx] = value;
		}
	}

	bool TableContains(const T &value, hash_t hash) const {
		auto tag = SlotTag(hash);
		for (auto idx = hash & mask; slot_tags[idx] != 0; idx = (idx + 1) & mask) {
			if (slot_tags[idx] == tag && Equals::Operation<T>(slot_values[idx], value)) {
				return true;
			}
		}
		return false;
	}

private:
	vector<T> collected;
	vector<uint64_t> bitmap;
	uint64_t bitmap_min = 0;
	uint64_t bitmap_range = 0;
	vector<hash_t> slot_tags;
	vector<T> slot_values;
	idx_t mask = 0;
	StringHeap heap;
};

template <>
string_t TemplatedInValueSet<string_t>::StoreValue(const string_t &value) {
	return heap.AddBlob(value);
}

unique_ptr<InValueSet> CreateInValueSet(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		return make_uniq<TemplatedInValueSet<bool>>();
	case PhysicalType::INT8:
		return make_uniq<TemplatedInValueSet<int8_t>>();
	case PhysicalType::INT16:
		return make_uniq<TemplatedInValueSet<int16_t>>();
	case PhysicalType::INT32:
		return make_uniq<TemplatedInValueSet<int32_t>>();
	case PhysicalType::INT64:
		return make_uniq<TemplatedInValueSet<int64_t>>();
	case PhysicalType::UINT8:
		return make_uniq<TemplatedInValueSet<uint8_t>>();
	case PhysicalType::UINT16:
		return make_uniq<TemplatedInValueSet<uint16_t>>();
	case PhysicalType::UINT32:
		return make_uniq<TemplatedInValueSet<uint32_t>>();
	case PhysicalType::UINT64:
		return make_uniq<TemplatedInValueSet<uint64_t>>();
	case PhysicalType::INT128:
		return make_uniq<TemplatedInValueSet<hugeint_t>>();
	case PhysicalType::UINT128:
		return make_uniq<TemplatedInValueSet<uhugeint_t>>();
	case PhysicalType::FLOAT:
		return make_uniq<TemplatedInValueSet<float>>();
	case PhysicalType::DOUBLE:
		return make_uniq<TemplatedInValueSet<double>>();
	case PhysicalType::INTERVAL:
		return make_uniq<TemplatedInValueSet<interval_t>>();
	case PhysicalType::VARCHAR:
		return make_uniq<TemplatedInValueSet<string_t>>();
	default:
		return nullptr;
	}
}

unique_ptr<InValueSet> TryCreateInValueSet(const BoundOperatorExpression &expr) {
	if (!InClauseRewriter::UsesHashLookup(expr)) {
		return nullptr;
	}
	auto &children = expr.GetChildren();
	auto &type = children[0]->GetReturnType();
	for (idx_t child_idx = 1; child_idx < children.size(); child_idx++) {
		if (children[child_idx]->GetReturnType() != type) {
			return nullptr;
		}
	}
	return CreateInValueSet(type);
}

struct InExpressionState : public ExpressionState {
	InExpressionState(const BoundOperatorExpression &expr, ExpressionExecutorState &root)
	    : ExpressionState(expr, root), value_set(TryCreateInValueSet(expr)) {
	}

	//! Filled from the constant children on the first non-empty chunk
	unique_ptr<InValueSet> value_set;
	bool value_set_filled = false;
};

} // namespace

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundOperatorExpression &expr,
                                                                ExpressionExecutorState &root) {
	unique_ptr<ExpressionState> result;
	if (expr.GetExpressionType() == ExpressionType::COMPARE_IN ||
	    expr.GetExpressionType() == ExpressionType::COMPARE_NOT_IN) {
		result = make_uniq<InExpressionState>(expr, root);
	} else {
		result = make_uniq<ExpressionState>(expr, root);
	}
	for (auto &child : expr.GetChildren()) {
		result->AddChild(*child);
	}

	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundOperatorExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	// special handling for special snowflake 'IN'
	// IN has n children
	auto expression_type = expr.GetExpressionType();
	if (expression_type == ExpressionType::COMPARE_IN || expression_type == ExpressionType::COMPARE_NOT_IN) {
		if (expr.GetChildren().size() < 2) {
			throw InvalidInputException("IN needs at least two children");
		}

		Vector left(expr.GetChildren()[0]->GetReturnType());
		// eval left side
		Execute(*expr.GetChildren()[0], state->child_states[0].get(), sel, count, left);

		auto &in_state = state->Cast<InExpressionState>();
		if (in_state.value_set && !in_state.value_set_filled && count > 0) {
			for (idx_t child = 1; child < expr.GetChildren().size(); child++) {
				Vector constant(expr.GetChildren()[child]->GetReturnType());
				Execute(*expr.GetChildren()[child], state->child_states[child].get(), nullptr, 1, constant);
				in_state.value_set->Add(constant);
			}
			in_state.value_set->Finalize();
			in_state.value_set_filled = true;
		}
		if (in_state.value_set_filled) {
			in_state.value_set->Probe(left, count, expression_type == ExpressionType::COMPARE_NOT_IN, result);
			return;
		}

		// init result to false
		Vector intermediate(LogicalType::BOOLEAN);
		intermediate.Reference(Value::BOOLEAN(false), count_t(count));

		// in rhs is a list of constants
		// for every child, OR the result of the comparison with the left
		// to get the overall result.
		for (idx_t child = 1; child < expr.GetChildren().size(); child++) {
			Vector vector_to_check(expr.GetChildren()[child]->GetReturnType());
			Vector comp_res(LogicalType::BOOLEAN);

			Execute(*expr.GetChildren()[child], state->child_states[child].get(), sel, count, vector_to_check);
			VectorOperations::Equals(left, vector_to_check, comp_res);

			if (child == 1) {
				// first child: move to result
				intermediate.Reference(comp_res);
			} else {
				// otherwise OR together
				Vector new_result(LogicalType::BOOLEAN);
				VectorOperations::Or(intermediate, comp_res, new_result);
				intermediate.Reference(new_result);
			}
		}
		if (expression_type == ExpressionType::COMPARE_NOT_IN) {
			// NOT IN: invert result
			VectorOperations::Not(intermediate, result);
		} else {
			// directly use the result
			result.Reference(intermediate);
		}
	} else if (expression_type == ExpressionType::OPERATOR_COALESCE) {
		SelectionVector sel_a(count);
		SelectionVector sel_b(count);
		SelectionVector slice_sel(count);
		SelectionVector result_sel(count);
		SelectionVector *next_sel = &sel_a;
		const SelectionVector *current_sel = sel;
		idx_t remaining_count = count;
		idx_t next_count;
		for (idx_t child = 0; child < expr.GetChildren().size(); child++) {
			Vector vector_to_check(expr.GetChildren()[child]->GetReturnType());
			Execute(*expr.GetChildren()[child], state->child_states[child].get(), current_sel, remaining_count,
			        vector_to_check);

			auto entries = vector_to_check.Validity();
			idx_t result_count = 0;
			next_count = 0;
			for (idx_t i = 0; i < remaining_count; i++) {
				auto base_idx = current_sel ? current_sel->get_index(i) : i;
				if (entries.IsValid(i)) {
					slice_sel.set_index(result_count, i);
					result_sel.set_index(result_count++, base_idx);
				} else {
					next_sel->set_index(next_count++, base_idx);
				}
			}
			if (result_count > 0) {
				vector_to_check.Slice(slice_sel, result_count);
				FillSwitch(vector_to_check, result, result_sel, NumericCast<sel_t>(result_count));
			}
			current_sel = next_sel;
			next_sel = next_sel == &sel_a ? &sel_b : &sel_a;
			remaining_count = next_count;
			if (next_count == 0) {
				break;
			}
		}
		if (remaining_count > 0) {
			for (idx_t i = 0; i < remaining_count; i++) {
				FlatVector::SetNull(result, current_sel->get_index(i), true);
			}
		}
		if (sel) {
			result.Slice(*sel, count);
		} else if (count == 1) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	} else if (expression_type == ExpressionType::OPERATOR_TRY) {
		auto &child_state = *state->child_states[0];
		Vector try_result(result.GetType());
		try {
			Execute(*expr.GetChildren()[0], &child_state, sel, count, try_result);
			if (try_result.GetVectorType() == VectorType::CONSTANT_VECTOR) {
				result.Reference(try_result);
				return;
			}
			VectorOperations::Copy(try_result, result, count, 0, 0);
			return;
		} catch (std::exception &ex) {
			ErrorData error(ex);
			auto error_type = error.Type();
			if (!Exception::IsExecutionError(error_type)) {
				throw;
			}
		}

		// On error, evaluate per row
		// CASE/COALESCE write their result at the physical row index, so the intermediate must fit that index
		SelectionVector selvec(1);
		DataChunk intermediate;
		intermediate.Initialize(GetAllocator(), {result.GetType()}, STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < count; i++) {
			intermediate.Reset();
			intermediate.SetChildCardinality(1);

			// Make sure to clear any dictionary states in the child expression, so that it actually
			// gets executed anew for every row
			child_state.ResetDictionaryStates();

			selvec.set_index(0, sel ? sel->get_index(i) : i);
			Value val(result.GetType());
			try {
				Execute(*expr.GetChildren()[0], &child_state, &selvec, 1, intermediate.data[0]);
				val = intermediate.GetValue(0, 0);
			} catch (std::exception &ex) {
				ErrorData error(ex);
				auto error_type = error.Type();
				if (!Exception::IsExecutionError(error_type)) {
					throw;
				}
			}
			result.SetValue(i, val);
		}
		if (count == 1) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	} else if (expr.GetChildren().size() == 1) {
		state->intermediate_chunk.Reset();
		auto &child = state->intermediate_chunk.data[0];

		Execute(*expr.GetChildren()[0], state->child_states[0].get(), sel, count, child);
		switch (expr.GetExpressionType()) {
		case ExpressionType::OPERATOR_NOT: {
			VectorOperations::Not(child, result);
			break;
		}
		case ExpressionType::OPERATOR_IS_NULL: {
			VectorOperations::IsNull(child, result);
			break;
		}
		case ExpressionType::OPERATOR_IS_NOT_NULL: {
			VectorOperations::IsNotNull(child, result);
			break;
		}
		default:
			throw NotImplementedException("Unsupported operator type with 1 child!");
		}
	} else {
		throw NotImplementedException("operator");
	}
}

} // namespace duckdb
