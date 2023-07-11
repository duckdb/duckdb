#include "duckdb/common/hive_partitioning.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "re2/re2.h"

namespace duckdb {

static unordered_map<column_t, string> GetKnownColumnValues(string &filename,
                                                            unordered_map<string, column_t> &column_map,
                                                            duckdb_re2::RE2 &compiled_regex, bool filename_col,
                                                            bool hive_partition_cols) {
	unordered_map<column_t, string> result;

	if (filename_col) {
		auto lookup_column_id = column_map.find("filename");
		if (lookup_column_id != column_map.end()) {
			result[lookup_column_id->second] = filename;
		}
	}

	if (hive_partition_cols) {
		auto partitions = HivePartitioning::Parse(filename, compiled_regex);
		for (auto &partition : partitions) {
			auto lookup_column_id = column_map.find(partition.first);
			if (lookup_column_id != column_map.end()) {
				result[lookup_column_id->second] = partition.second;
			}
		}
	}

	return result;
}

// Takes an expression and converts a list of known column_refs to constants
static void ConvertKnownColRefToConstants(unique_ptr<Expression> &expr,
                                          unordered_map<column_t, string> &known_column_values, idx_t table_index) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = expr->Cast<BoundColumnRefExpression>();

		// This bound column ref is for another table
		if (table_index != bound_colref.binding.table_index) {
			return;
		}

		auto lookup = known_column_values.find(bound_colref.binding.column_index);
		if (lookup != known_column_values.end()) {
			expr = make_uniq<BoundConstantExpression>(Value(lookup->second).DefaultCastAs(bound_colref.return_type));
		}
	} else {
		ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
			ConvertKnownColRefToConstants(child, known_column_values, table_index);
		});
	}
}

// matches hive partitions in file name. For example:
// 	- s3://bucket/var1=value1/bla/bla/var2=value2
//  - http(s)://domain(:port)/lala/kasdl/var1=value1/?not-a-var=not-a-value
//  - folder/folder/folder/../var1=value1/etc/.//var2=value2
const string HivePartitioning::REGEX_STRING = "[\\/\\\\]([^\\/\\?\\\\]+)=([^\\/\\n\\?\\\\]+)";

std::map<string, string> HivePartitioning::Parse(const string &filename, duckdb_re2::RE2 &regex) {
	std::map<string, string> result;
	duckdb_re2::StringPiece input(filename); // Wrap a StringPiece around it

	string var;
	string value;
	while (RE2::FindAndConsume(&input, regex, &var, &value)) {
		result.insert(std::pair<string, string>(var, value));
	}
	return result;
}

std::map<string, string> HivePartitioning::Parse(const string &filename) {
	duckdb_re2::RE2 regex(REGEX_STRING);
	return Parse(filename, regex);
}

// TODO: this can still be improved by removing the parts of filter expressions that are true for all remaining files.
//		 currently, only expressions that cannot be evaluated during pushdown are removed.
void HivePartitioning::ApplyFiltersToFileList(ClientContext &context, vector<string> &files,
                                              vector<unique_ptr<Expression>> &filters,
                                              unordered_map<string, column_t> &column_map, LogicalGet &get,
                                              bool hive_enabled, bool filename_enabled) {

	vector<string> pruned_files;
	vector<bool> have_preserved_filter(filters.size(), false);
	vector<unique_ptr<Expression>> pruned_filters;
	unordered_set<idx_t> filters_applied_to_files;
	duckdb_re2::RE2 regex(REGEX_STRING);
	auto table_index = get.table_index;

	if ((!filename_enabled && !hive_enabled) || filters.empty()) {
		return;
	}

	for (idx_t i = 0; i < files.size(); i++) {
		auto &file = files[i];
		bool should_prune_file = false;
		auto known_values = GetKnownColumnValues(file, column_map, regex, filename_enabled, hive_enabled);

		FilterCombiner combiner(context);

		for (idx_t j = 0; j < filters.size(); j++) {
			auto &filter = filters[j];
			unique_ptr<Expression> filter_copy = filter->Copy();
			ConvertKnownColRefToConstants(filter_copy, known_values, table_index);
			// Evaluate the filter, if it can be evaluated here, we can not prune this filter
			Value result_value;

			if (!filter_copy->IsScalar() || !filter_copy->IsFoldable() ||
			    !ExpressionExecutor::TryEvaluateScalar(context, *filter_copy, result_value)) {
				// can not be evaluated only with the filename/hive columns added, we can not prune this filter
				if (!have_preserved_filter[j]) {
					pruned_filters.emplace_back(filter->Copy());
					have_preserved_filter[j] = true;
				}
			} else if (!result_value.GetValue<bool>()) {
				// filter evaluates to false
				should_prune_file = true;
				// convert the filter to a table filter.
				if (filters_applied_to_files.find(j) == filters_applied_to_files.end()) {
					get.extra_info.file_filters += filter->ToString();
					filters_applied_to_files.insert(j);
				}
			}
		}

		if (!should_prune_file) {
			pruned_files.push_back(file);
		}
	}

	D_ASSERT(filters.size() >= pruned_filters.size());

	filters = std::move(pruned_filters);
	files = std::move(pruned_files);
}

HivePartitionedColumnData::HivePartitionedColumnData(const HivePartitionedColumnData &other)
    : PartitionedColumnData(other), hashes_v(LogicalType::HASH) {
	// Synchronize to ensure consistency of shared partition map
	if (other.global_state) {
		global_state = other.global_state;
		unique_lock<mutex> lck(global_state->lock);
		SynchronizeLocalMap();
	}
	InitializeKeys();
}

void HivePartitionedColumnData::InitializeKeys() {
	keys.resize(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		keys[i].values.resize(group_by_columns.size());
	}
}

template <class T>
static inline Value GetHiveKeyValue(const T &val) {
	return Value::CreateValue<T>(val);
}

template <class T>
static inline Value GetHiveKeyValue(const T &val, const LogicalType &type) {
	auto result = GetHiveKeyValue(val);
	result.Reinterpret(type);
	return result;
}

static inline Value GetHiveKeyNullValue(const LogicalType &type) {
	Value result;
	result.Reinterpret(type);
	return result;
}

template <class T>
static void TemplatedGetHivePartitionValues(Vector &input, vector<HivePartitionKey> &keys, const idx_t col_idx,
                                            const idx_t count) {
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(count, format);

	const auto &sel = *format.sel;
	const auto data = UnifiedVectorFormat::GetData<T>(format);
	const auto &validity = format.validity;

	const auto &type = input.GetType();

	const auto reinterpret = Value::CreateValue<T>(data[0]).GetTypeMutable() != type;
	if (reinterpret) {
		for (idx_t i = 0; i < count; i++) {
			auto &key = keys[i];
			const auto idx = sel.get_index(i);
			if (validity.RowIsValid(idx)) {
				key.values[col_idx] = GetHiveKeyValue(data[idx], type);
			} else {
				key.values[col_idx] = GetHiveKeyNullValue(type);
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto &key = keys[i];
			const auto idx = sel.get_index(i);
			if (validity.RowIsValid(idx)) {
				key.values[col_idx] = GetHiveKeyValue(data[idx]);
			} else {
				key.values[col_idx] = GetHiveKeyNullValue(type);
			}
		}
	}
}

static void GetNestedHivePartitionValues(Vector &input, vector<HivePartitionKey> &keys, const idx_t col_idx,
                                         const idx_t count) {
	for (idx_t i = 0; i < count; i++) {
		auto &key = keys[i];
		key.values[col_idx] = input.GetValue(i);
	}
}

static void GetHivePartitionValuesTypeSwitch(Vector &input, vector<HivePartitionKey> &keys, const idx_t col_idx,
                                             const idx_t count) {
	const auto &type = input.GetType();
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		TemplatedGetHivePartitionValues<bool>(input, keys, col_idx, count);
		break;
	case PhysicalType::INT8:
		TemplatedGetHivePartitionValues<int8_t>(input, keys, col_idx, count);
		break;
	case PhysicalType::INT16:
		TemplatedGetHivePartitionValues<int16_t>(input, keys, col_idx, count);
		break;
	case PhysicalType::INT32:
		TemplatedGetHivePartitionValues<int32_t>(input, keys, col_idx, count);
		break;
	case PhysicalType::INT64:
		TemplatedGetHivePartitionValues<int64_t>(input, keys, col_idx, count);
		break;
	case PhysicalType::INT128:
		TemplatedGetHivePartitionValues<hugeint_t>(input, keys, col_idx, count);
		break;
	case PhysicalType::UINT8:
		TemplatedGetHivePartitionValues<uint8_t>(input, keys, col_idx, count);
		break;
	case PhysicalType::UINT16:
		TemplatedGetHivePartitionValues<uint16_t>(input, keys, col_idx, count);
		break;
	case PhysicalType::UINT32:
		TemplatedGetHivePartitionValues<uint32_t>(input, keys, col_idx, count);
		break;
	case PhysicalType::UINT64:
		TemplatedGetHivePartitionValues<uint64_t>(input, keys, col_idx, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedGetHivePartitionValues<float>(input, keys, col_idx, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedGetHivePartitionValues<double>(input, keys, col_idx, count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedGetHivePartitionValues<interval_t>(input, keys, col_idx, count);
		break;
	case PhysicalType::VARCHAR:
		TemplatedGetHivePartitionValues<string_t>(input, keys, col_idx, count);
		break;
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
		GetNestedHivePartitionValues(input, keys, col_idx, count);
		break;
	default:
		throw InternalException("Unsupported type for HivePartitionedColumnData::ComputePartitionIndices");
	}
}

void HivePartitionedColumnData::ComputePartitionIndices(PartitionedColumnDataAppendState &state, DataChunk &input) {
	const auto count = input.size();

	input.Hash(group_by_columns, hashes_v);
	hashes_v.Flatten(count);

	for (idx_t col_idx = 0; col_idx < group_by_columns.size(); col_idx++) {
		auto &group_by_col = input.data[group_by_columns[col_idx]];
		GetHivePartitionValuesTypeSwitch(group_by_col, keys, col_idx, count);
	}

	const auto hashes = FlatVector::GetData<hash_t>(hashes_v);
	const auto partition_indices = FlatVector::GetData<idx_t>(state.partition_indices);
	for (idx_t i = 0; i < count; i++) {
		auto &key = keys[i];
		key.hash = hashes[i];
		auto lookup = local_partition_map.find(key);
		if (lookup == local_partition_map.end()) {
			idx_t new_partition_id = RegisterNewPartition(key, state);
			partition_indices[i] = new_partition_id;
		} else {
			partition_indices[i] = lookup->second;
		}
	}
}

std::map<idx_t, const HivePartitionKey *> HivePartitionedColumnData::GetReverseMap() {
	std::map<idx_t, const HivePartitionKey *> ret;
	for (const auto &pair : local_partition_map) {
		ret[pair.second] = &(pair.first);
	}
	return ret;
}

void HivePartitionedColumnData::GrowAllocators() {
	unique_lock<mutex> lck_gstate(allocators->lock);

	idx_t current_allocator_size = allocators->allocators.size();
	idx_t required_allocators = local_partition_map.size();

	allocators->allocators.reserve(current_allocator_size);
	for (idx_t i = current_allocator_size; i < required_allocators; i++) {
		CreateAllocator();
	}

	D_ASSERT(allocators->allocators.size() == local_partition_map.size());
}

void HivePartitionedColumnData::GrowAppendState(PartitionedColumnDataAppendState &state) {
	idx_t current_append_state_size = state.partition_append_states.size();
	idx_t required_append_state_size = local_partition_map.size();

	for (idx_t i = current_append_state_size; i < required_append_state_size; i++) {
		state.partition_append_states.emplace_back(make_uniq<ColumnDataAppendState>());
		state.partition_buffers.emplace_back(CreatePartitionBuffer());
	}
}

void HivePartitionedColumnData::GrowPartitions(PartitionedColumnDataAppendState &state) {
	idx_t current_partitions = partitions.size();
	idx_t required_partitions = local_partition_map.size();

	D_ASSERT(allocators->allocators.size() == required_partitions);

	for (idx_t i = current_partitions; i < required_partitions; i++) {
		partitions.emplace_back(CreatePartitionCollection(i));
		partitions[i]->InitializeAppend(*state.partition_append_states[i]);
	}
	D_ASSERT(partitions.size() == local_partition_map.size());
}

void HivePartitionedColumnData::SynchronizeLocalMap() {
	// Synchronise global map into local, may contain changes from other threads too
	for (auto it = global_state->partitions.begin() + local_partition_map.size(); it < global_state->partitions.end();
	     it++) {
		local_partition_map[(*it)->first] = (*it)->second;
	}
}

idx_t HivePartitionedColumnData::RegisterNewPartition(HivePartitionKey key, PartitionedColumnDataAppendState &state) {
	if (global_state) {
		idx_t partition_id;

		// Synchronize Global state with our local state with the newly discoveren partition
		{
			unique_lock<mutex> lck_gstate(global_state->lock);

			// Insert into global map, or return partition if already present
			auto res =
			    global_state->partition_map.emplace(std::make_pair(std::move(key), global_state->partition_map.size()));
			auto it = res.first;
			partition_id = it->second;

			// Add iterator to vector to allow incrementally updating local states from global state
			global_state->partitions.emplace_back(it);
			SynchronizeLocalMap();
		}

		// After synchronizing with the global state, we need to grow the shared allocators to support
		// the number of partitions, which guarantees that there's always enough allocators available to each thread
		GrowAllocators();

		// Grow local partition data
		GrowAppendState(state);
		GrowPartitions(state);

		return partition_id;
	} else {
		return local_partition_map.emplace(std::make_pair(std::move(key), local_partition_map.size())).first->second;
	}
}

} // namespace duckdb
