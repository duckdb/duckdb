#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"

#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"

namespace duckdb {

PerfectHashJoinExecutor::PerfectHashJoinExecutor(const PhysicalHashJoin &join_p, JoinHashTable &ht_p)
    : join(join_p), ht(ht_p) {
}

//===--------------------------------------------------------------------===//
// Initialize
//===--------------------------------------------------------------------===//
bool ExtractNumericValue(Value val, hugeint_t &result) {
	if (!val.type().IsIntegral()) {
		switch (val.type().InternalType()) {
		case PhysicalType::INT8:
			result = Hugeint::Convert(val.GetValueUnsafe<int8_t>());
			break;
		case PhysicalType::INT16:
			result = Hugeint::Convert(val.GetValueUnsafe<int16_t>());
			break;
		case PhysicalType::INT32:
			result = Hugeint::Convert(val.GetValueUnsafe<int32_t>());
			break;
		case PhysicalType::INT64:
			result = Hugeint::Convert(val.GetValueUnsafe<int64_t>());
			break;
		case PhysicalType::INT128:
			result = val.GetValueUnsafe<hugeint_t>();
			break;
		case PhysicalType::UINT8:
			result = Hugeint::Convert(val.GetValueUnsafe<uint8_t>());
			break;
		case PhysicalType::UINT16:
			result = Hugeint::Convert(val.GetValueUnsafe<uint16_t>());
			break;
		case PhysicalType::UINT32:
			result = Hugeint::Convert(val.GetValueUnsafe<uint32_t>());
			break;
		case PhysicalType::UINT64:
			result = Hugeint::Convert(val.GetValueUnsafe<uint64_t>());
			break;
		case PhysicalType::UINT128: {
			const auto uhugeint_val = val.GetValueUnsafe<uhugeint_t>();
			if (uhugeint_val > NumericCast<uhugeint_t>(NumericLimits<hugeint_t>::Maximum())) {
				return false;
			}
			result.lower = uhugeint_val.lower;
			result.upper = NumericCast<int64_t>(uhugeint_val.upper);
			break;
		}
		default:
			return false;
		}
	} else {
		if (!val.DefaultTryCastAs(LogicalType::HUGEINT)) {
			return false;
		}
		result = val.GetValue<hugeint_t>();
	}
	return true;
}

bool PerfectHashJoinExecutor::CanDoPerfectHashJoin(const PhysicalHashJoin &op, const Value &min, const Value &max) {
	if (perfect_join_statistics.is_build_small) {
		return true; // Already true based on static statistics
	}

	// We only do this optimization for inner joins with one integer equality condition
	const auto key_type = op.conditions[0].left->return_type;
	if (op.join_type != JoinType::INNER || op.conditions.size() != 1 ||
	    op.conditions[0].comparison != ExpressionType::COMPARE_EQUAL || !TypeIsInteger(key_type.InternalType())) {
		return false;
	}

	// We bail out if there are nested types on the RHS
	for (auto &type : op.children[1]->types) {
		switch (type.InternalType()) {
		case PhysicalType::STRUCT:
		case PhysicalType::LIST:
		case PhysicalType::ARRAY:
			return false;
		default:
			break;
		}
	}

	// And when the build range is smaller than the threshold
	perfect_join_statistics.build_min = min;
	perfect_join_statistics.build_max = max;
	hugeint_t min_value, max_value;
	if (!ExtractNumericValue(perfect_join_statistics.build_min, min_value) ||
	    !ExtractNumericValue(perfect_join_statistics.build_max, max_value)) {
		return false;
	}
	if (max_value < min_value) {
		return false; // Empty table
	}

	hugeint_t build_range;
	if (!TrySubtractOperator::Operation(max_value, min_value, build_range)) {
		return false;
	}

	// The max size our build must have to run the perfect HJ
	static constexpr idx_t MAX_BUILD_SIZE = 1048576;
	if (build_range > Hugeint::Convert(MAX_BUILD_SIZE)) {
		return false;
	}
	perfect_join_statistics.build_range = NumericCast<idx_t>(build_range);

	// If count is larger than range (duplicates), we bail out
	if (ht.Count() > perfect_join_statistics.build_range) {
		return false;
	}

	perfect_join_statistics.is_build_small = true;
	return true;
}

//===--------------------------------------------------------------------===//
// Build
//===--------------------------------------------------------------------===//
bool PerfectHashJoinExecutor::BuildPerfectHashTable(LogicalType &key_type) {
	// First, allocate memory for each build column
	auto build_size = perfect_join_statistics.build_range + 1;
	for (const auto &type : join.rhs_output_columns.col_types) {
		perfect_hash_table.emplace_back(type, build_size);
	}

	// and for duplicate_checking
	bitmap_build_idx = make_unsafe_uniq_array_uninitialized<bool>(build_size);
	memset(bitmap_build_idx.get(), 0, sizeof(bool) * build_size); // set false

	// Now fill columns with build data
	return FullScanHashTable(key_type);
}

bool PerfectHashJoinExecutor::FullScanHashTable(LogicalType &key_type) {
	auto &data_collection = ht.GetDataCollection();

	// TODO: In a parallel finalize: One should exclusively lock and each thread should do one part of the code below.
	Vector tuples_addresses(LogicalType::POINTER, ht.Count()); // allocate space for all the tuples

	idx_t key_count = 0;
	if (data_collection.ChunkCount() > 0) {
		JoinHTScanState join_ht_state(data_collection, 0, data_collection.ChunkCount(),
		                              TupleDataPinProperties::KEEP_EVERYTHING_PINNED);

		// Go through all the blocks and fill the keys addresses
		key_count = ht.FillWithHTOffsets(join_ht_state, tuples_addresses);
	}

	// Scan the build keys in the hash table
	Vector build_vector(key_type, key_count);
	data_collection.Gather(tuples_addresses, *FlatVector::IncrementalSelectionVector(), key_count, 0, build_vector,
	                       *FlatVector::IncrementalSelectionVector(), nullptr);

	// Now fill the selection vector using the build keys and create a sequential vector
	// TODO: add check for fast pass when probe is part of build domain
	SelectionVector sel_build(key_count + 1);
	SelectionVector sel_tuples(key_count + 1);
	bool success = FillSelectionVectorSwitchBuild(build_vector, sel_build, sel_tuples, key_count);

	// early out
	if (!success) {
		return false;
	}
	if (unique_keys == perfect_join_statistics.build_range + 1 && !ht.has_null) {
		perfect_join_statistics.is_build_dense = true;
	}
	key_count = unique_keys; // do not consider keys out of the range

	// Full scan the remaining build columns and fill the perfect hash table
	const auto build_size = perfect_join_statistics.build_range + 1;
	for (idx_t i = 0; i < join.rhs_output_columns.col_types.size(); i++) {
		auto &vector = perfect_hash_table[i];
		const auto output_col_idx = ht.output_columns[i];
		D_ASSERT(vector.GetType() == ht.layout.GetTypes()[output_col_idx]);
		if (build_size > STANDARD_VECTOR_SIZE) {
			auto &col_mask = FlatVector::Validity(vector);
			col_mask.Initialize(build_size);
		}
		data_collection.Gather(tuples_addresses, sel_tuples, key_count, output_col_idx, vector, sel_build, nullptr);
	}

	return true;
}

bool PerfectHashJoinExecutor::FillSelectionVectorSwitchBuild(Vector &source, SelectionVector &sel_vec,
                                                             SelectionVector &seq_sel_vec, idx_t count) {
	switch (source.GetType().InternalType()) {
	case PhysicalType::INT8:
		return TemplatedFillSelectionVectorBuild<int8_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::INT16:
		return TemplatedFillSelectionVectorBuild<int16_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::INT32:
		return TemplatedFillSelectionVectorBuild<int32_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::INT64:
		return TemplatedFillSelectionVectorBuild<int64_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::INT128:
		return TemplatedFillSelectionVectorBuild<hugeint_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::UINT8:
		return TemplatedFillSelectionVectorBuild<uint8_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::UINT16:
		return TemplatedFillSelectionVectorBuild<uint16_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::UINT32:
		return TemplatedFillSelectionVectorBuild<uint32_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::UINT64:
		return TemplatedFillSelectionVectorBuild<uint64_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::UINT128:
		return TemplatedFillSelectionVectorBuild<uhugeint_t>(source, sel_vec, seq_sel_vec, count);
	default:
		throw NotImplementedException("Type not supported for perfect hash join");
	}
}

template <typename T>
bool PerfectHashJoinExecutor::TemplatedFillSelectionVectorBuild(Vector &source, SelectionVector &sel_vec,
                                                                SelectionVector &seq_sel_vec, idx_t count) {
	if (perfect_join_statistics.build_min.IsNull() || perfect_join_statistics.build_max.IsNull()) {
		return false;
	}
	auto min_value = perfect_join_statistics.build_min.GetValueUnsafe<T>();
	auto max_value = perfect_join_statistics.build_max.GetValueUnsafe<T>();
	UnifiedVectorFormat vector_data;
	source.ToUnifiedFormat(count, vector_data);
	auto data = reinterpret_cast<T *>(vector_data.data);
	// generate the selection vector
	for (idx_t i = 0, sel_idx = 0; i < count; ++i) {
		auto data_idx = vector_data.sel->get_index(i);
		auto input_value = data[data_idx];
		// add index to selection vector if value in the range
		if (min_value <= input_value && input_value <= max_value) {
			auto idx = (idx_t)(input_value - min_value); // subtract min value to get the idx position
			sel_vec.set_index(sel_idx, idx);
			if (bitmap_build_idx[idx]) {
				return false;
			} else {
				bitmap_build_idx[idx] = true;
				unique_keys++;
			}
			seq_sel_vec.set_index(sel_idx++, i);
		}
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Probe
//===--------------------------------------------------------------------===//
class PerfectHashJoinState : public OperatorState {
public:
	PerfectHashJoinState(ClientContext &context, const PhysicalHashJoin &join) : probe_executor(context) {
		join_keys.Initialize(Allocator::Get(context), join.condition_types);
		for (auto &cond : join.conditions) {
			probe_executor.AddExpression(*cond.left);
		}
		build_sel_vec.Initialize(STANDARD_VECTOR_SIZE);
		probe_sel_vec.Initialize(STANDARD_VECTOR_SIZE);
		seq_sel_vec.Initialize(STANDARD_VECTOR_SIZE);
	}

	DataChunk join_keys;
	ExpressionExecutor probe_executor;
	SelectionVector build_sel_vec;
	SelectionVector probe_sel_vec;
	SelectionVector seq_sel_vec;
};

unique_ptr<OperatorState> PerfectHashJoinExecutor::GetOperatorState(ExecutionContext &context) {
	auto state = make_uniq<PerfectHashJoinState>(context.client, join);
	return std::move(state);
}

OperatorResultType PerfectHashJoinExecutor::ProbePerfectHashTable(ExecutionContext &context, DataChunk &input,
                                                                  DataChunk &lhs_output_columns, DataChunk &result,
                                                                  OperatorState &state_p) {
	auto &state = state_p.Cast<PerfectHashJoinState>();
	// keeps track of how many probe keys have a match
	idx_t probe_sel_count = 0;

	// fetch the join keys from the chunk
	state.join_keys.Reset();
	state.probe_executor.Execute(input, state.join_keys);
	// select the keys that are in the min-max range
	auto &keys_vec = state.join_keys.data[0];
	auto keys_count = state.join_keys.size();
	// todo: add check for fast pass when probe is part of build domain
	FillSelectionVectorSwitchProbe(keys_vec, state.build_sel_vec, state.probe_sel_vec, keys_count, probe_sel_count);

	// If build is dense and probe is in build's domain, just reference probe
	if (perfect_join_statistics.is_build_dense && keys_count == probe_sel_count) {
		result.Reference(lhs_output_columns);
	} else {
		// otherwise, filter it out the values that do not match
		result.Slice(lhs_output_columns, state.probe_sel_vec, probe_sel_count, 0);
	}
	// on the build side, we need to fetch the data and build dictionary vectors with the sel_vec
	for (idx_t i = 0; i < join.rhs_output_columns.col_types.size(); i++) {
		auto &result_vector = result.data[lhs_output_columns.ColumnCount() + i];
		D_ASSERT(result_vector.GetType() == ht.layout.GetTypes()[ht.output_columns[i]]);
		auto &build_vec = perfect_hash_table[i];
		result_vector.Reference(build_vec);
		result_vector.Slice(state.build_sel_vec, probe_sel_count);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

void PerfectHashJoinExecutor::FillSelectionVectorSwitchProbe(Vector &source, SelectionVector &build_sel_vec,
                                                             SelectionVector &probe_sel_vec, idx_t count,
                                                             idx_t &probe_sel_count) {
	switch (source.GetType().InternalType()) {
	case PhysicalType::INT8:
		TemplatedFillSelectionVectorProbe<int8_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::INT16:
		TemplatedFillSelectionVectorProbe<int16_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::INT32:
		TemplatedFillSelectionVectorProbe<int32_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::INT64:
		TemplatedFillSelectionVectorProbe<int64_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::INT128:
		TemplatedFillSelectionVectorProbe<hugeint_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::UINT8:
		TemplatedFillSelectionVectorProbe<uint8_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::UINT16:
		TemplatedFillSelectionVectorProbe<uint16_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::UINT32:
		TemplatedFillSelectionVectorProbe<uint32_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::UINT64:
		TemplatedFillSelectionVectorProbe<uint64_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::UINT128:
		TemplatedFillSelectionVectorProbe<uhugeint_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	default:
		throw NotImplementedException("Type not supported");
	}
}

template <typename T>
void PerfectHashJoinExecutor::TemplatedFillSelectionVectorProbe(Vector &source, SelectionVector &build_sel_vec,
                                                                SelectionVector &probe_sel_vec, idx_t count,
                                                                idx_t &probe_sel_count) {
	auto min_value = perfect_join_statistics.build_min.GetValueUnsafe<T>();
	auto max_value = perfect_join_statistics.build_max.GetValueUnsafe<T>();

	UnifiedVectorFormat vector_data;
	source.ToUnifiedFormat(count, vector_data);
	auto data = reinterpret_cast<T *>(vector_data.data);
	auto validity_mask = &vector_data.validity;
	// build selection vector for non-dense build
	if (validity_mask->AllValid()) {
		for (idx_t i = 0, sel_idx = 0; i < count; ++i) {
			// retrieve value from vector
			auto data_idx = vector_data.sel->get_index(i);
			auto input_value = data[data_idx];
			// add index to selection vector if value in the range
			if (min_value <= input_value && input_value <= max_value) {
				auto idx = (idx_t)(input_value - min_value); // subtract min value to get the idx position
				                                             // check for matches in the build
				if (bitmap_build_idx[idx]) {
					build_sel_vec.set_index(sel_idx, idx);
					probe_sel_vec.set_index(sel_idx++, i);
					probe_sel_count++;
				}
			}
		}
	} else {
		for (idx_t i = 0, sel_idx = 0; i < count; ++i) {
			// retrieve value from vector
			auto data_idx = vector_data.sel->get_index(i);
			if (!validity_mask->RowIsValid(data_idx)) {
				continue;
			}
			auto input_value = data[data_idx];
			// add index to selection vector if value in the range
			if (min_value <= input_value && input_value <= max_value) {
				auto idx = (idx_t)(input_value - min_value); // subtract min value to get the idx position
				                                             // check for matches in the build
				if (bitmap_build_idx[idx]) {
					build_sel_vec.set_index(sel_idx, idx);
					probe_sel_vec.set_index(sel_idx++, i);
					probe_sel_count++;
				}
			}
		}
	}
}

} // namespace duckdb
