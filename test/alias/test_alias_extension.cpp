
#include "test_alias_extension.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"

namespace duckdb {

void TestAliasHello(DataChunk &args, ExpressionState &state, Vector &result) {
	result.Reference(Value("Hello Alias!"));
}

static void AddPointOrStructFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &left_vector = args.data[0];
	auto &right_vector = args.data[1];
	const int count = args.size();

	auto left_vector_type = left_vector.GetVectorType();
	auto right_vector_type = right_vector.GetVectorType();

	auto &child_entries = StructVector::GetEntries(result);
	if (left_vector_type == VectorType::CONSTANT_VECTOR && right_vector_type == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		if (ConstantVector::IsNull(left_vector)) {
			ConstantVector::SetNull(result, true);
		} else {
			ConstantVector::SetNull(result, false);
		}
		auto &left_child_entries = StructVector::GetEntries(left_vector);
		auto &right_child_entries = StructVector::GetEntries(right_vector);
		for (int base_idx = 0; base_idx < count; base_idx++) {
			for (size_t col = 0; col < child_entries.size(); ++col) {
				auto &child_entry = child_entries[col];
				auto &left_child_entry = left_child_entries[col];
				auto &right_child_entry = right_child_entries[col];
				auto pdata = ConstantVector::GetData<int32_t>(*child_entry);
				auto left_pdata = ConstantVector::GetData<int32_t>(*left_child_entry);
				auto right_pdata = ConstantVector::GetData<int32_t>(*right_child_entry);
				pdata[base_idx] = left_pdata[base_idx] + right_pdata[base_idx];
			}
		}
	} else if ((left_vector_type == VectorType::FLAT_VECTOR && right_vector_type == VectorType::CONSTANT_VECTOR) ||
	           (left_vector_type == VectorType::CONSTANT_VECTOR && right_vector_type == VectorType::FLAT_VECTOR)) {
		auto &constant_vector = left_vector_type == VectorType::FLAT_VECTOR ? args.data[1] : args.data[0];
		auto &flat_vector = left_vector_type == VectorType::FLAT_VECTOR ? args.data[0] : args.data[1];
		if (ConstantVector::IsNull(constant_vector)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
			return;
		}
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto &constant_child_entries = StructVector::GetEntries(constant_vector);
		auto &flat_child_entries = StructVector::GetEntries(flat_vector);
		auto csize = child_entries.size();
		std::vector<int32_t> constant_vec;
		for (size_t col = 0; col < csize; ++col) {
			auto &constant_child_entry = constant_child_entries[col];
			auto constant_pdata = ConstantVector::GetData<int32_t>(*constant_child_entry);
			constant_vec.push_back(constant_pdata[0]);
		}
		for (int base_idx = 0; base_idx < count; base_idx++) {
			if (FlatVector::IsNull(flat_vector, base_idx)) {
				FlatVector::SetNull(result, base_idx, true);
				continue;
			}
			for (size_t col = 0; col < csize; ++col) {
				auto &child_entry = child_entries[col];
				auto &flat_child_entry = flat_child_entries[col];
				auto pdata = ConstantVector::GetData<int32_t>(*child_entry);
				auto flat_pdata = ConstantVector::GetData<int32_t>(*flat_child_entry);
				pdata[base_idx] = constant_vec[col] + flat_pdata[base_idx];
			}
		}
	} else {
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto &left_child_entries = StructVector::GetEntries(left_vector);
		auto &right_child_entries = StructVector::GetEntries(right_vector);
		for (int base_idx = 0; base_idx < count; base_idx++) {
			if (FlatVector::IsNull(left_vector, base_idx) || FlatVector::IsNull(right_vector, base_idx)) {
				FlatVector::SetNull(result, base_idx, true);
				continue;
			}
			for (size_t col = 0; col < child_entries.size(); ++col) {
				auto &child_entry = child_entries[col];
				auto &left_child_entry = left_child_entries[col];
				auto &right_child_entry = right_child_entries[col];
				auto pdata = ConstantVector::GetData<int32_t>(*child_entry);
				auto left_pdata = ConstantVector::GetData<int32_t>(*left_child_entry);
				auto right_pdata = ConstantVector::GetData<int32_t>(*right_child_entry);
				pdata[base_idx] = left_pdata[base_idx] + right_pdata[base_idx];
			}
		}
	}
	result.Verify(count);
}

static void SubPointOrStructFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &left_vector = args.data[0];
	auto &right_vector = args.data[1];
	const int count = args.size();

	auto left_vector_type = left_vector.GetVectorType();
	auto right_vector_type = right_vector.GetVectorType();

	auto &child_entries = StructVector::GetEntries(result);
	if (left_vector_type == VectorType::CONSTANT_VECTOR && right_vector_type == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		if (ConstantVector::IsNull(left_vector)) {
			ConstantVector::SetNull(result, true);
		} else {
			ConstantVector::SetNull(result, false);
		}
		auto &left_child_entries = StructVector::GetEntries(left_vector);
		auto &right_child_entries = StructVector::GetEntries(right_vector);
		for (int base_idx = 0; base_idx < count; base_idx++) {
			for (size_t col = 0; col < child_entries.size(); ++col) {
				auto &child_entry = child_entries[col];
				auto &left_child_entry = left_child_entries[col];
				auto &right_child_entry = right_child_entries[col];
				auto pdata = ConstantVector::GetData<int32_t>(*child_entry);
				auto left_pdata = ConstantVector::GetData<int32_t>(*left_child_entry);
				auto right_pdata = ConstantVector::GetData<int32_t>(*right_child_entry);
				pdata[base_idx] = left_pdata[base_idx] - right_pdata[base_idx];
			}
		}
	} else if ((left_vector_type == VectorType::FLAT_VECTOR && right_vector_type == VectorType::CONSTANT_VECTOR) ||
	           (left_vector_type == VectorType::CONSTANT_VECTOR && right_vector_type == VectorType::FLAT_VECTOR)) {
		auto constant_left = left_vector_type == VectorType::CONSTANT_VECTOR;
		auto &constant_vector = left_vector_type == VectorType::FLAT_VECTOR ? args.data[1] : args.data[0];
		auto &flat_vector = left_vector_type == VectorType::FLAT_VECTOR ? args.data[0] : args.data[1];
		if (ConstantVector::IsNull(constant_vector)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
			return;
		}
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto &constant_child_entries = StructVector::GetEntries(constant_vector);
		auto &flat_child_entries = StructVector::GetEntries(flat_vector);
		auto csize = child_entries.size();
		std::vector<int32_t> constant_vec;
		for (size_t col = 0; col < csize; ++col) {
			auto &constant_child_entry = constant_child_entries[col];
			auto constant_pdata = ConstantVector::GetData<int32_t>(*constant_child_entry);
			constant_vec.push_back(constant_pdata[0]);
		}
		for (int base_idx = 0; base_idx < count; base_idx++) {
			if (FlatVector::IsNull(flat_vector, base_idx)) {
				FlatVector::SetNull(result, base_idx, true);
				continue;
			}
			for (size_t col = 0; col < csize; ++col) {
				auto &child_entry = child_entries[col];
				auto &flat_child_entry = flat_child_entries[col];
				auto pdata = ConstantVector::GetData<int32_t>(*child_entry);
				auto flat_pdata = ConstantVector::GetData<int32_t>(*flat_child_entry);
				pdata[base_idx] = constant_left ? (constant_vec[col] - flat_pdata[base_idx])
				                                : (flat_pdata[base_idx] - constant_vec[col]);
			}
		}
	} else {
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto &left_child_entries = StructVector::GetEntries(left_vector);
		auto &right_child_entries = StructVector::GetEntries(right_vector);
		for (int base_idx = 0; base_idx < count; base_idx++) {
			if (FlatVector::IsNull(left_vector, base_idx) || FlatVector::IsNull(right_vector, base_idx)) {
				FlatVector::SetNull(result, base_idx, true);
				continue;
			}
			for (size_t col = 0; col < child_entries.size(); ++col) {
				auto &child_entry = child_entries[col];
				auto &left_child_entry = left_child_entries[col];
				auto &right_child_entry = right_child_entries[col];
				auto pdata = ConstantVector::GetData<int32_t>(*child_entry);
				auto left_pdata = ConstantVector::GetData<int32_t>(*left_child_entry);
				auto right_pdata = ConstantVector::GetData<int32_t>(*right_child_entry);
				pdata[base_idx] = left_pdata[base_idx] - right_pdata[base_idx];
			}
		}
	}
	result.Verify(count);
}

void TestAliasExtension::Load(DuckDB &db) {
	CreateScalarFunctionInfo hello_alias_info(
	    ScalarFunction("test_alias_hello", {}, LogicalType::VARCHAR, TestAliasHello));

	Connection conn(db);
	conn.BeginTransaction();
	auto &client_context = *conn.context;
	auto &catalog = Catalog::GetCatalog(client_context);
	catalog.CreateFunction(client_context, &hello_alias_info);

	// Add alias POINT type
	string alias_name = "POINT";
	child_list_t<LogicalType> child_types;
	child_types.push_back(make_pair("x", LogicalType::INTEGER));
	child_types.push_back(make_pair("y", LogicalType::INTEGER));
	auto alias_info = make_unique<CreateTypeInfo>();
	alias_info->name = alias_name;
	LogicalType target_type = LogicalType::STRUCT(child_types);
	target_type.SetAlias(alias_name);
	alias_info->type = target_type;

	auto entry = (TypeCatalogEntry *)catalog.CreateType(client_context, alias_info.get());
	LogicalType::SetCatalog(target_type, entry);

	// Function add point
	ScalarFunction add_point_func("add_point", {target_type, target_type}, target_type, AddPointOrStructFunction);
	CreateScalarFunctionInfo add_point_info(add_point_func);
	catalog.CreateFunction(client_context, &add_point_info);

	// Function sub point
	ScalarFunction sub_point_func("sub_point", {target_type, target_type}, target_type, SubPointOrStructFunction);
	CreateScalarFunctionInfo sub_point_info(sub_point_func);
	catalog.CreateFunction(client_context, &sub_point_info);

	// Function add struct 2D
	auto struct_type = LogicalType::STRUCT(child_types);
	ScalarFunction add_struct_func("add_struct", {struct_type, struct_type}, struct_type, AddPointOrStructFunction);
	CreateScalarFunctionInfo add_struct_info(add_struct_func);
	catalog.CreateFunction(client_context, &add_struct_info);

	// Function substitute struct 2D
	ScalarFunction sub_struct_func("sub_struct", {struct_type, struct_type}, struct_type, SubPointOrStructFunction);
	CreateScalarFunctionInfo sub_struct_info(sub_struct_func);
	catalog.CreateFunction(client_context, &sub_struct_info);

	conn.Commit();
}

std::string TestAliasExtension::Name() {
	return "test_alias";
}

} // namespace duckdb
