#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/cast_rules.hpp"
#include "excel-extension.hpp"
#include "nf_calendar.h"
#include "nf_localedata.h"
#include "nf_zformat.h"
#include "variant_value.hpp"

namespace duckdb {

static std::string GetNumberFormatString(std::string &format, double num_value) {
	duckdb_excel::LocaleData locale_data;
	duckdb_excel::ImpSvNumberInputScan input_scan(&locale_data);
	uint16_t nCheckPos;
	std::string out_str;

	duckdb_excel::SvNumberformat num_format(format, &locale_data, &input_scan, nCheckPos);

	if (!num_format.GetOutputString(num_value, out_str)) {
		return out_str;
	}

	return "";
}

static string_t NumberFormatScalarFunction(Vector &result, double num_value, string_t format) {
	try {
		string in_str = format.GetString();
		string out_str = GetNumberFormatString(in_str, num_value);

		if (out_str.length() > 0) {
			auto result_string = StringVector::EmptyString(result, out_str.size());
			auto result_data = result_string.GetDataWriteable();
			memcpy(result_data, out_str.c_str(), out_str.size());
			result_string.Finalize();
			return result_string;
		} else {
			auto result_string = StringVector::EmptyString(result, 0);
			result_string.Finalize();
			return result_string;
		}
	} catch (...) {
		throw InternalException("Unexpected result for number format");
	}

	return string_t();
}

static void NumberFormatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &number_vector = args.data[0];
	auto &format_vector = args.data[1];
	BinaryExecutor::Execute<double, string_t, string_t>(
	    number_vector, format_vector, result, args.size(),
	    [&](double value, string_t format) { return NumberFormatScalarFunction(result, value, format); });
}

static void VariantFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetVectorType(args.data[0].GetVectorType() == VectorType::CONSTANT_VECTOR ? VectorType::CONSTANT_VECTOR
	                                                                                 : VectorType::FLAT_VECTOR);
	for (idx_t i = 0; i < args.size(); ++i) {
		result.SetValue(i, Variant(args.GetValue(0, i)));
	}
}

static void FromVariantFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	bool is_constant = true;
	for (idx_t i = 1; i < args.ColumnCount(); ++i) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			is_constant = false;
			break;
		}
	}
	result.SetVectorType(is_constant ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	auto &type = result.GetType();
	for (idx_t i_row = 0; i_row < args.size(); ++i_row) {
		Value val = FromVariant(args.GetValue(1, i_row));
		const Value *vp = &val;
		for (idx_t i_idx = 2; i_idx < args.ColumnCount(); ++i_idx) {
			if (vp->IsNull()) {
				goto set_null;
			}
			switch (vp->type().id()) {
			case LogicalTypeId::STRUCT: {
				if (args.data[i_idx].GetType().id() != LogicalTypeId::VARCHAR) {
					goto set_null;
				}
				Value val_idx = args.GetValue(i_idx, i_row);
				if (val_idx.IsNull()) {
					goto set_null;
				}
				auto &child_types = StructType::GetChildTypes(vp->type());
				for (idx_t i = 0;; ++i) {
					if (i >= child_types.size()) {
						goto set_null;
					}
					if (child_types[i].first == StringValue::Get(val_idx)) {
						vp = &StructValue::GetChildren(*vp)[i];
						break;
					}
				}
				break;
			}
			case LogicalTypeId::LIST: {
				if (args.data[i_idx].GetType().id() == LogicalTypeId::VARCHAR) {
					goto set_null;
				}
				Value val_idx = args.GetValue(i_idx, i_row);
				if (val_idx.IsNull()) {
					goto set_null;
				}
				variant_index_type idx = val_idx.GetValue<variant_index_type>();
				if (idx >= ListValue::GetChildren(*vp).size()) {
					goto set_null;
				}
				vp = &ListValue::GetChildren(*vp)[idx];
				break;
			}
			default:
				goto set_null;
			}
		}
		if (vp->IsNull()) {
			goto set_null;
		}
		if (vp->type() != type) {
			auto cost = CastRules::ImplicitCast(vp->type(), type);
			if (cost < 0 || cost > 120 || !const_cast<Value *>(vp)->TryCastAs(type)) {
				goto set_null;
			}
		}
		result.SetValue(i_row, *vp);
		continue;
	set_null:
		if (is_constant) {
			ConstantVector::SetNull(result, true);
		} else {
			FlatVector::SetNull(result, i_row, true);
		}
	}
}

unique_ptr<FunctionData> FromVariantBind(ClientContext &context, ScalarFunction &bound_function,
                                         vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[0]->IsFoldable()) {
		throw InvalidInputException("type must be a constant");
	}
	Value type_str = ExpressionExecutor::EvaluateScalar(*arguments[0]);
	if (type_str.IsNull() || type_str.type().id() != LogicalTypeId::VARCHAR) {
		throw InvalidInputException("invalid type");
	}
	for (idx_t i = 2; i < arguments.size(); ++i) {
		const auto &type = arguments[i]->return_type;
		if (type.id() != LogicalTypeId::VARCHAR && !type.IsIntegral()) {
			throw InvalidInputException("indices must be of string or integer type");
		}
	}
	LogicalTypeId return_type_id = TransformStringToLogicalTypeId(StringValue::Get(type_str));
	if (return_type_id == LogicalTypeId::USER) {
		bound_function.return_type = TransformStringToLogicalType(StringValue::Get(type_str));
	} else {
		bound_function.return_type = return_type_id;
	}
	return nullptr;
}

void EXCELExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();

	auto &catalog = Catalog::GetCatalog(*con.context);

	ScalarFunction text_func("text", {LogicalType::DOUBLE, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                         NumberFormatFunction);
	CreateScalarFunctionInfo text_info(text_func);
	catalog.CreateFunction(*con.context, &text_info);

	ScalarFunction excel_text_func("excel_text", {LogicalType::DOUBLE, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                               NumberFormatFunction);
	CreateScalarFunctionInfo excel_text_info(excel_text_func);
	catalog.CreateFunction(*con.context, &excel_text_info);

	CreateScalarFunctionInfo variant_info(
	    ScalarFunction("variant", {LogicalType::ANY}, LogicalType::BLOB, VariantFunction));
	catalog.CreateFunction(*con.context, &variant_info);

	CreateScalarFunctionInfo from_variant_info(
	    ScalarFunction("from_variant", {LogicalType::VARCHAR, LogicalType::BLOB}, LogicalType::ANY, FromVariantFunction,
	                   false, FromVariantBind, nullptr, nullptr, nullptr, LogicalType::ANY));
	catalog.CreateFunction(*con.context, &from_variant_info);

	con.Commit();
}

std::string EXCELExtension::Name() {
	return "excel";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void excel_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::EXCELExtension>();
}

DUCKDB_EXTENSION_API const char *excel_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
