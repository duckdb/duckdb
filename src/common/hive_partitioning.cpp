#include "duckdb/common/hive_partitioning.hpp"

#include "duckdb/common/uhugeint.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/multi_file_list.hpp"

namespace duckdb {

struct PartitioningColumnValue {
	explicit PartitioningColumnValue(string value_p) : value(std::move(value_p)) {
	}
	PartitioningColumnValue(string key_p, string value_p) : key(std::move(key_p)), value(std::move(value_p)) {
	}

	string key;
	string value;
};

static unordered_map<column_t, PartitioningColumnValue>
GetKnownColumnValues(const string &filename, const HivePartitioningFilterInfo &filter_info) {
	unordered_map<column_t, PartitioningColumnValue> result;

	auto &column_map = filter_info.column_map;
	if (filter_info.filename_enabled) {
		auto lookup_column_id = column_map.find("filename");
		if (lookup_column_id != column_map.end()) {
			result.insert(make_pair(lookup_column_id->second, PartitioningColumnValue(filename)));
		}
	}

	if (filter_info.hive_enabled) {
		auto partitions = HivePartitioning::Parse(filename);
		for (auto &partition : partitions) {
			auto lookup_column_id = column_map.find(partition.first);
			if (lookup_column_id != column_map.end()) {
				result.insert(
				    make_pair(lookup_column_id->second, PartitioningColumnValue(partition.first, partition.second)));
			}
		}
	}

	return result;
}

// Takes an expression and converts a list of known column_refs to constants
static void ConvertKnownColRefToConstants(ClientContext &context, unique_ptr<Expression> &expr,
                                          const unordered_map<column_t, PartitioningColumnValue> &known_column_values,
                                          idx_t table_index) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = expr->Cast<BoundColumnRefExpression>();

		// This bound column ref is for another table
		if (table_index != bound_colref.binding.table_index) {
			return;
		}

		auto lookup = known_column_values.find(bound_colref.binding.column_index);
		if (lookup != known_column_values.end()) {
			auto &partition_val = lookup->second;
			Value result_val;
			if (partition_val.key.empty()) {
				// filename column - use directly
				result_val = Value(partition_val.value);
			} else {
				// hive partitioning column - cast the value to the target type
				result_val = HivePartitioning::GetValue(context, partition_val.key, partition_val.value,
				                                        bound_colref.return_type);
			}
			expr = make_uniq<BoundConstantExpression>(std::move(result_val));
		}
	} else {
		ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
			ConvertKnownColRefToConstants(context, child, known_column_values, table_index);
		});
	}
}

string HivePartitioning::Escape(const string &input) {
	return StringUtil::URLEncode(input);
}

string HivePartitioning::Unescape(const string &input) {
	return StringUtil::URLDecode(input);
}

// matches hive partitions in file name. For example:
// 	- s3://bucket/var1=value1/bla/bla/var2=value2
//  - http(s)://domain(:port)/lala/kasdl/var1=value1/?not-a-var=not-a-value
//  - folder/folder/folder/../var1=value1/etc/.//var2=value2
std::map<string, string> HivePartitioning::Parse(const string &filename) {
	idx_t partition_start = 0;
	idx_t equality_sign = 0;
	bool candidate_partition = true;
	std::map<string, string> result;
	for (idx_t c = 0; c < filename.size(); c++) {
		if (filename[c] == '?' || filename[c] == '\n') {
			// get parameter or newline - not a partition
			candidate_partition = false;
		}
		if (filename[c] == '\\' || filename[c] == '/') {
			// separator
			if (candidate_partition && equality_sign > partition_start) {
				// we found a partition with an equality sign
				string key = filename.substr(partition_start, equality_sign - partition_start);
				string value = filename.substr(equality_sign + 1, c - equality_sign - 1);
				result.insert(make_pair(std::move(key), std::move(value)));
			}
			partition_start = c + 1;
			candidate_partition = true;
		} else if (filename[c] == '=') {
			if (equality_sign > partition_start) {
				// multiple equality signs - not a partition
				candidate_partition = false;
			}
			equality_sign = c;
		}
	}
	return result;
}

Value HivePartitioning::GetValue(ClientContext &context, const string &key, const string &str_val,
                                 const LogicalType &type) {
	// Handle nulls
	if (StringUtil::CIEquals(str_val, "NULL")) {
		return Value(type);
	}
	if (type.id() == LogicalTypeId::VARCHAR) {
		// for string values we can directly return the type
		return Value(Unescape(str_val));
	}
	if (str_val.empty()) {
		// empty strings are NULL for non-string types
		return Value(type);
	}

	// cast to the target type
	Value value(Unescape(str_val));
	if (!value.TryCastAs(context, type)) {
		throw InvalidInputException("Unable to cast '%s' (from hive partition column '%s') to: '%s'", value.ToString(),
		                            StringUtil::Upper(key), type.ToString());
	}
	return value;
}

// TODO: this can still be improved by removing the parts of filter expressions that are true for all remaining files.
//		 currently, only expressions that cannot be evaluated during pushdown are removed.
void HivePartitioning::ApplyFiltersToFileList(ClientContext &context, vector<string> &files,
                                              vector<unique_ptr<Expression>> &filters,
                                              const HivePartitioningFilterInfo &filter_info,
                                              MultiFilePushdownInfo &info) {

	vector<string> pruned_files;
	vector<bool> have_preserved_filter(filters.size(), false);
	vector<unique_ptr<Expression>> pruned_filters;
	unordered_set<idx_t> filters_applied_to_files;
	auto table_index = info.table_index;

	if ((!filter_info.filename_enabled && !filter_info.hive_enabled) || filters.empty()) {
		return;
	}

	for (idx_t i = 0; i < files.size(); i++) {
		auto &file = files[i];
		bool should_prune_file = false;
		auto known_values = GetKnownColumnValues(file, filter_info);

		for (idx_t j = 0; j < filters.size(); j++) {
			auto &filter = filters[j];
			unique_ptr<Expression> filter_copy = filter->Copy();
			ConvertKnownColRefToConstants(context, filter_copy, known_values, table_index);
			// Evaluate the filter, if it can be evaluated here, we can not prune this filter
			Value result_value;

			if (!filter_copy->IsScalar() || !filter_copy->IsFoldable() ||
			    !ExpressionExecutor::TryEvaluateScalar(context, *filter_copy, result_value)) {
				// can not be evaluated only with the filename/hive columns added, we can not prune this filter
				if (!have_preserved_filter[j]) {
					pruned_filters.emplace_back(filter->Copy());
					have_preserved_filter[j] = true;
				}
			} else if (result_value.IsNull() || !result_value.GetValue<bool>()) {
				// filter evaluates to false
				should_prune_file = true;
				// convert the filter to a table filter.
				if (filters_applied_to_files.find(j) == filters_applied_to_files.end()) {
					info.extra_info.file_filters += filter->ToString();
					filters_applied_to_files.insert(j);
				}
			}
		}

		if (!should_prune_file) {
			pruned_files.push_back(file);
		}
	}

	D_ASSERT(filters.size() >= pruned_filters.size());

	info.extra_info.total_files = files.size();
	info.extra_info.filtered_files = pruned_files.size();

	filters = std::move(pruned_filters);
	files = std::move(pruned_files);
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
	case PhysicalType::UINT128:
		TemplatedGetHivePartitionValues<uhugeint_t>(input, keys, col_idx, count);
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

HivePartitionedColumnData::HivePartitionedColumnData(ClientContext &context, vector<LogicalType> types,
                                                     vector<idx_t> partition_by_cols,
                                                     shared_ptr<GlobalHivePartitionState> global_state)
    : PartitionedColumnData(PartitionedColumnDataType::HIVE, context, std::move(types)),
      global_state(std::move(global_state)), group_by_columns(std::move(partition_by_cols)),
      hashes_v(LogicalType::HASH) {
	InitializeKeys();
	CreateAllocator();
}

void HivePartitionedColumnData::AddNewPartition(HivePartitionKey key, idx_t partition_id,
                                                PartitionedColumnDataAppendState &state) {
	local_partition_map.emplace(std::move(key), partition_id);

	if (state.partition_append_states.size() <= partition_id) {
		state.partition_append_states.resize(partition_id + 1);
		state.partition_buffers.resize(partition_id + 1);
		partitions.resize(partition_id + 1);
	}
	state.partition_append_states[partition_id] = make_uniq<ColumnDataAppendState>();
	state.partition_buffers[partition_id] = CreatePartitionBuffer();
	partitions[partition_id] = CreatePartitionCollection(0);
	partitions[partition_id]->InitializeAppend(*state.partition_append_states[partition_id]);
}

idx_t HivePartitionedColumnData::RegisterNewPartition(HivePartitionKey key, PartitionedColumnDataAppendState &state) {
	idx_t partition_id;
	if (global_state) {
		// Synchronize Global state with our local state with the newly discovered partition
		unique_lock<mutex> lck_gstate(global_state->lock);

		// Insert into global map, or return partition if already present
		auto res = global_state->partition_map.emplace(std::make_pair(key, global_state->partition_map.size()));
		partition_id = res.first->second;
	} else {
		partition_id = local_partition_map.size();
	}
	AddNewPartition(std::move(key), partition_id, state);
	return partition_id;
}

} // namespace duckdb
