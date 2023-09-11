#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

// FLAT, CONSTANT, DICTIONARY, SEQUENCE
struct TestVectorBindData : public TableFunctionData {
	vector<LogicalType> types;
	bool all_flat = false;
};

struct TestVectorTypesData : public GlobalTableFunctionState {
	TestVectorTypesData() : offset(0) {
	}

	vector<unique_ptr<DataChunk>> entries;
	idx_t offset;
};

struct TestVectorInfo {
	TestVectorInfo(const vector<LogicalType> &types, const map<LogicalTypeId, TestType> &test_type_map,
	               vector<unique_ptr<DataChunk>> &entries)
	    : types(types), test_type_map(test_type_map), entries(entries) {
	}

	const vector<LogicalType> &types;
	const map<LogicalTypeId, TestType> &test_type_map;
	vector<unique_ptr<DataChunk>> &entries;
};

struct TestGeneratedValues {
public:
	void AddColumn(vector<Value> values) {
		if (!column_values.empty() && column_values[0].size() != values.size()) {
			throw InternalException("Size mismatch when adding a column to TestGeneratedValues");
		}
		column_values.push_back(std::move(values));
	}

	const Value &GetValue(idx_t row, idx_t column) const {
		return column_values[column][row];
	}

	idx_t Rows() const {
		return column_values.empty() ? 0 : column_values[0].size();
	}

	idx_t Columns() const {
		return column_values.size();
	}

private:
	vector<vector<Value>> column_values;
};

struct TestVectorFlat {
	static constexpr const idx_t TEST_VECTOR_CARDINALITY = 3;

	static vector<Value> GenerateValues(TestVectorInfo &info, const LogicalType &type) {
		vector<Value> result;
		switch (type.InternalType()) {
		case PhysicalType::STRUCT: {
			vector<child_list_t<Value>> struct_children;
			auto &child_types = StructType::GetChildTypes(type);

			struct_children.resize(TEST_VECTOR_CARDINALITY);
			for (auto &child_type : child_types) {
				auto child_values = GenerateValues(info, child_type.second);

				for (idx_t i = 0; i < child_values.size(); i++) {
					struct_children[i].push_back(make_pair(child_type.first, std::move(child_values[i])));
				}
			}
			for (auto &struct_child : struct_children) {
				result.push_back(Value::STRUCT(std::move(struct_child)));
			}
			break;
		}
		case PhysicalType::LIST: {
			auto &child_type = ListType::GetChildType(type);
			auto child_values = GenerateValues(info, child_type);

			result.push_back(Value::LIST(child_type, {child_values[0], child_values[1]}));
			result.push_back(Value::LIST(child_type, {}));
			result.push_back(Value::LIST(child_type, {child_values[2]}));
			break;
		}
		default: {
			auto entry = info.test_type_map.find(type.id());
			if (entry == info.test_type_map.end()) {
				throw NotImplementedException("Unimplemented type for test_vector_types %s", type.ToString());
			}
			result.push_back(entry->second.min_value);
			result.push_back(entry->second.max_value);
			result.emplace_back(type);
			break;
		}
		}
		return result;
	}

	static TestGeneratedValues GenerateValues(TestVectorInfo &info) {
		// generate the values for each column
		TestGeneratedValues generated_values;
		for (auto &type : info.types) {
			generated_values.AddColumn(GenerateValues(info, type));
		}
		return generated_values;
	}

	static void Generate(TestVectorInfo &info) {
		auto result_values = GenerateValues(info);
		for (idx_t cur_row = 0; cur_row < result_values.Rows(); cur_row += STANDARD_VECTOR_SIZE) {
			auto result = make_uniq<DataChunk>();
			result->Initialize(Allocator::DefaultAllocator(), info.types);
			auto cardinality = MinValue<idx_t>(STANDARD_VECTOR_SIZE, result_values.Rows() - cur_row);
			for (idx_t c = 0; c < info.types.size(); c++) {
				for (idx_t i = 0; i < cardinality; i++) {
					result->data[c].SetValue(i, result_values.GetValue(cur_row + i, c));
				}
			}
			result->SetCardinality(cardinality);
			info.entries.push_back(std::move(result));
		}
	}
};

struct TestVectorConstant {
	static void Generate(TestVectorInfo &info) {
		auto values = TestVectorFlat::GenerateValues(info);
		for (idx_t cur_row = 0; cur_row < TestVectorFlat::TEST_VECTOR_CARDINALITY; cur_row += STANDARD_VECTOR_SIZE) {
			auto result = make_uniq<DataChunk>();
			result->Initialize(Allocator::DefaultAllocator(), info.types);
			auto cardinality = MinValue<idx_t>(STANDARD_VECTOR_SIZE, TestVectorFlat::TEST_VECTOR_CARDINALITY - cur_row);
			for (idx_t c = 0; c < info.types.size(); c++) {
				result->data[c].SetValue(0, values.GetValue(0, c));
				result->data[c].SetVectorType(VectorType::CONSTANT_VECTOR);
			}
			result->SetCardinality(cardinality);

			info.entries.push_back(std::move(result));
		}
	}
};

struct TestVectorSequence {
	static void GenerateVector(TestVectorInfo &info, const LogicalType &type, Vector &result) {
		D_ASSERT(type == result.GetType());
		switch (type.id()) {
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT:
			result.Sequence(3, 2, 3);
			return;
		default:
			break;
		}
		switch (type.InternalType()) {
		case PhysicalType::STRUCT: {
			auto &child_entries = StructVector::GetEntries(result);
			for (auto &child_entry : child_entries) {
				GenerateVector(info, child_entry->GetType(), *child_entry);
			}
			break;
		}
		case PhysicalType::LIST: {
			auto data = FlatVector::GetData<list_entry_t>(result);
			data[0].offset = 0;
			data[0].length = 2;
			data[1].offset = 2;
			data[1].length = 0;
			data[2].offset = 2;
			data[2].length = 1;

			GenerateVector(info, ListType::GetChildType(type), ListVector::GetEntry(result));
			ListVector::SetListSize(result, 3);
			break;
		}
		default: {
			auto entry = info.test_type_map.find(type.id());
			if (entry == info.test_type_map.end()) {
				throw NotImplementedException("Unimplemented type for test_vector_types %s", type.ToString());
			}
			result.SetValue(0, entry->second.min_value);
			result.SetValue(1, entry->second.max_value);
			result.SetValue(2, Value(type));
			break;
		}
		}
	}

	static void Generate(TestVectorInfo &info) {
#if STANDARD_VECTOR_SIZE > 2
		auto result = make_uniq<DataChunk>();
		result->Initialize(Allocator::DefaultAllocator(), info.types);

		for (idx_t c = 0; c < info.types.size(); c++) {
			GenerateVector(info, info.types[c], result->data[c]);
		}
		result->SetCardinality(3);
		info.entries.push_back(std::move(result));
#endif
	}
};

struct TestVectorDictionary {
	static void Generate(TestVectorInfo &info) {
		idx_t current_chunk = info.entries.size();

		unordered_set<idx_t> slice_entries {1, 2};

		TestVectorFlat::Generate(info);
		idx_t current_idx = 0;
		for (idx_t i = current_chunk; i < info.entries.size(); i++) {
			auto &chunk = *info.entries[i];
			SelectionVector sel(STANDARD_VECTOR_SIZE);
			idx_t sel_idx = 0;
			for (idx_t k = 0; k < chunk.size(); k++) {
				if (slice_entries.count(current_idx + k) > 0) {
					sel.set_index(sel_idx++, k);
				}
			}
			chunk.Slice(sel, sel_idx);
			current_idx += chunk.size();
		}
	}
};

static unique_ptr<FunctionData> TestVectorTypesBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<TestVectorBindData>();
	for (idx_t i = 0; i < input.inputs.size(); i++) {
		string name = "test_vector";
		if (i > 0) {
			name += to_string(i + 1);
		}
		auto &input_val = input.inputs[i];
		names.emplace_back(name);
		return_types.push_back(input_val.type());
		result->types.push_back(input_val.type());
	}
	for (auto &entry : input.named_parameters) {
		if (entry.first == "all_flat") {
			result->all_flat = BooleanValue::Get(entry.second);
		} else {
			throw InternalException("Unrecognized named parameter for test_vector_types");
		}
	}
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> TestVectorTypesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<TestVectorBindData>();

	auto result = make_uniq<TestVectorTypesData>();

	auto test_types = TestAllTypesFun::GetTestTypes();

	map<LogicalTypeId, TestType> test_type_map;
	for (auto &test_type : test_types) {
		test_type_map.insert(make_pair(test_type.type.id(), std::move(test_type)));
	}

	TestVectorInfo info(bind_data.types, test_type_map, result->entries);
	TestVectorFlat::Generate(info);
	TestVectorConstant::Generate(info);
	TestVectorDictionary::Generate(info);
	TestVectorSequence::Generate(info);
	for (auto &entry : result->entries) {
		entry->Verify();
	}
	if (bind_data.all_flat) {
		for (auto &entry : result->entries) {
			entry->Flatten();
			entry->Verify();
		}
	}
	return std::move(result);
}

void TestVectorTypesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<TestVectorTypesData>();
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	output.Reference(*data.entries[data.offset]);
	data.offset++;
}

void TestVectorTypesFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunction test_vector_types("test_vector_types", {LogicalType::ANY}, TestVectorTypesFunction,
	                                TestVectorTypesBind, TestVectorTypesInit);
	test_vector_types.varargs = LogicalType::ANY;
	test_vector_types.named_parameters["all_flat"] = LogicalType::BOOLEAN;

	set.AddFunction(std::move(test_vector_types));
}

} // namespace duckdb
