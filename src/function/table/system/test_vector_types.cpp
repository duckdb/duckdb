#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

// FLAT, CONSTANT, DICTIONARY, SEQUENCE
struct TestVectorBindData : public TableFunctionData {
	LogicalType type;
	bool all_flat;
};

struct TestVectorTypesData : public GlobalTableFunctionState {
	TestVectorTypesData() : offset(0) {
	}

	vector<unique_ptr<DataChunk>> entries;
	idx_t offset;
};

struct TestVectorInfo {
	TestVectorInfo(const LogicalType &type, const map<LogicalTypeId, TestType> &test_type_map,
	               vector<unique_ptr<DataChunk>> &entries)
	    : type(type), test_type_map(test_type_map), entries(entries) {
	}

	const LogicalType &type;
	const map<LogicalTypeId, TestType> &test_type_map;
	vector<unique_ptr<DataChunk>> &entries;
};

struct TestVectorFlat {
	static void GenerateVector(TestVectorInfo &info, const LogicalType &type, Vector &result) {
		D_ASSERT(type == result.GetType());
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
		auto result = make_unique<DataChunk>();
		result->Initialize({info.type});

		GenerateVector(info, info.type, result->data[0]);
		result->SetCardinality(3);
		info.entries.push_back(move(result));
	}
};

struct TestVectorConstant {
	static void GenerateVector(TestVectorInfo &info, const LogicalType &type, Vector &result) {
		D_ASSERT(type == result.GetType());
		switch (type.InternalType()) {
		case PhysicalType::STRUCT: {
			auto &child_entries = StructVector::GetEntries(result);
			for (auto &child_entry : child_entries) {
				GenerateVector(info, child_entry->GetType(), *child_entry);
			}
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			break;
		}
		case PhysicalType::LIST: {
			auto data = FlatVector::GetData<list_entry_t>(result);
			data[0].offset = 0;
			data[0].length = 1;

			GenerateVector(info, ListType::GetChildType(type), ListVector::GetEntry(result));
			ListVector::SetListSize(result, 1);
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			break;
		}
		default: {
			auto entry = info.test_type_map.find(type.id());
			if (entry == info.test_type_map.end()) {
				throw NotImplementedException("Unimplemented type for test_vector_types %s", type.ToString());
			}
			result.SetValue(0, entry->second.min_value);
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			break;
		}
		}
	}

	static void Generate(TestVectorInfo &info) {
		auto result = make_unique<DataChunk>();
		result->Initialize({info.type});

		GenerateVector(info, info.type, result->data[0]);
		result->SetCardinality(3);
		info.entries.push_back(move(result));
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
			result.Sequence(3, 2);
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
		auto result = make_unique<DataChunk>();
		result->Initialize({info.type});

		GenerateVector(info, info.type, result->data[0]);
		result->SetCardinality(3);
		info.entries.push_back(move(result));
	}
};

struct TestVectorDictionary {
	static void Generate(TestVectorInfo &info) {
		auto result = make_unique<DataChunk>();
		result->Initialize({info.type});

		TestVectorFlat::GenerateVector(info, info.type, result->data[0]);
		result->SetCardinality(3);

		SelectionVector sel(STANDARD_VECTOR_SIZE);
		sel.set_index(0, 1);
		sel.set_index(1, 2);
		result->Slice(sel, 2);
		info.entries.push_back(move(result));
	}
};

static unique_ptr<FunctionData> TestVectorTypesBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_unique<TestVectorBindData>();
	result->type = input.inputs[0].type();
	result->all_flat = BooleanValue::Get(input.inputs[1]);

	return_types.push_back(result->type);
	names.emplace_back("test_vector");
	return move(result);
}

unique_ptr<GlobalTableFunctionState> TestVectorTypesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = (TestVectorBindData &)*input.bind_data;

	auto result = make_unique<TestVectorTypesData>();

	auto test_types = TestAllTypesFun::GetTestTypes();

	map<LogicalTypeId, TestType> test_type_map;
	for (auto &test_type : test_types) {
		test_type_map.insert(make_pair(test_type.type.id(), move(test_type)));
	}

	TestVectorInfo info(bind_data.type, test_type_map, result->entries);
	TestVectorFlat::Generate(info);
	TestVectorConstant::Generate(info);
	TestVectorDictionary::Generate(info);
	TestVectorSequence::Generate(info);
	for (auto &entry : result->entries) {
		entry->Verify();
	}
	if (bind_data.all_flat) {
		for (auto &entry : result->entries) {
			entry->Normalify();
			entry->Verify();
		}
	}
	return move(result);
}

void TestVectorTypesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (TestVectorTypesData &)*data_p.global_state;
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	output.Reference(*data.entries[data.offset]);
	data.offset++;
}

void TestVectorTypesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("test_vector_types", {LogicalType::ANY, LogicalType::BOOLEAN},
	                              TestVectorTypesFunction, TestVectorTypesBind, TestVectorTypesInit));
}

} // namespace duckdb
