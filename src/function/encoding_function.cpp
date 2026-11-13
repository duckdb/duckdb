#include "duckdb/function/encoding_function.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

string EncodingFunction::FunctionName(const string &encoding_name) {
	return StringUtil::Lower(encoding_name);
}

void EncodingFunction::SetSchema(CreateFunctionInfo &info) {
	info.SetQualifiedName(
	    QualifiedName(Identifier::InvalidCatalog(), Identifier(SCHEMA), info.GetQualifiedName().Name()));
}

LogicalType EncodingFunction::StreamResultType() {
	child_list_t<LogicalType> children;
	children.emplace_back("decoded", LogicalType::VARCHAR);
	children.emplace_back("consumed", LogicalType::BIGINT);
	return LogicalType::STRUCT(std::move(children));
}

ScalarFunctionSet EncodingFunction::Create(const string &encoding_name, decode_t decode, const encode_t &encode,
                                           idx_t minimum_input_window, idx_t max_output_bytes) {
	D_ASSERT(decode && minimum_input_window > 0 && max_output_bytes > 0);
	const Identifier name(FunctionName(encoding_name));

	ScalarFunction decode_all(name, vector<LogicalType> {LogicalType::BLOB}, LogicalType::VARCHAR,
	                          [decode, max_output_bytes](DataChunk &args, ExpressionState &, Vector &result) {
		                          auto inputs = args.data[0].Values<string_t>();
		                          auto writer = FlatVector::Writer<string_t>(result, args.size());
		                          for (auto entry : inputs) {
			                          if (!entry.IsValid()) {
				                          writer.WriteNull();
				                          continue;
			                          }
			                          auto input = entry.GetValue();
			                          const idx_t capacity = input.GetSize() * max_output_bytes;
			                          auto &output = writer.WriteEmptyString(capacity);
			                          idx_t consumed = 0;
			                          idx_t written = 0;
			                          decode(input.GetData(), input.GetSize(), true, output.GetDataWriteable(),
			                                 consumed, written);
			                          output.SetSizeAndFinalize(UnsafeNumericCast<uint32_t>(written), capacity);
		                          }
	                          });
	decode_all.GetSignature().GetParameter(0).SetName(Identifier("data"));
	decode_all.SetFallible();
	decode_all.GetProperties().SetProperty(MINIMUM_INPUT_WINDOW_PROPERTY,
	                                       Value::BIGINT(NumericCast<int64_t>(minimum_input_window)));

	ScalarFunction decode_stream(
	    name, vector<LogicalType> {LogicalType::BLOB, LogicalType::BOOLEAN}, StreamResultType(),
	    [decode, max_output_bytes](DataChunk &args, ExpressionState &, Vector &result) {
		    auto inputs = args.data[0].Values<string_t>();
		    auto is_last = args.data[1].Values<bool>();
		    auto writer = FlatVector::Writer<VectorStructType<string_t, int64_t>>(result, args.size());
		    for (auto entry : inputs) {
			    if (!entry.IsValid() || !is_last[entry.GetIndex()].IsValid()) {
				    writer.WriteNull();
				    continue;
			    }
			    auto input = entry.GetValue();
			    const bool last = is_last[entry.GetIndex()].GetValue();
			    writer.WriteValue([&](auto &decoded_writer, auto &consumed_writer) {
				    const idx_t capacity = input.GetSize() * max_output_bytes;
				    auto &output = decoded_writer.WriteEmptyString(capacity);
				    idx_t consumed = 0;
				    idx_t written = 0;
				    decode(input.GetData(), input.GetSize(), last, output.GetDataWriteable(), consumed, written);
				    output.SetSizeAndFinalize(UnsafeNumericCast<uint32_t>(written), capacity);
				    consumed_writer.WriteValue(UnsafeNumericCast<int64_t>(consumed));
			    });
		    }
	    });
	decode_stream.GetSignature().GetParameter(0).SetName(Identifier("data"));
	decode_stream.GetSignature().GetParameter(1).SetName(Identifier("is_last"));
	decode_stream.SetFallible();
	decode_stream.SetExtraFunctionInfo<EncodingFunctionInfo>(decode, max_output_bytes);
	decode_stream.GetProperties().SetProperty(MINIMUM_INPUT_WINDOW_PROPERTY,
	                                          Value::BIGINT(NumericCast<int64_t>(minimum_input_window)));

	ScalarFunctionSet set(name);
	set.AddFunction(std::move(decode_all));
	set.AddFunction(std::move(decode_stream));
	if (encode) {
		ScalarFunction encode_all(name, vector<LogicalType> {LogicalType::VARCHAR}, LogicalType::BLOB,
		                          [encode, minimum_input_window](DataChunk &args, ExpressionState &, Vector &result) {
			                          auto inputs = args.data[0].Values<string_t>();
			                          auto writer = FlatVector::Writer<string_t>(result, args.size());
			                          for (auto entry : inputs) {
				                          if (!entry.IsValid()) {
					                          writer.WriteNull();
					                          continue;
				                          }
				                          auto input = entry.GetValue();
				                          const idx_t capacity = input.GetSize() * minimum_input_window;
				                          auto &output = writer.WriteEmptyString(capacity);
				                          idx_t written = 0;
				                          encode(input.GetData(), input.GetSize(), output.GetDataWriteable(), written);
				                          output.SetSizeAndFinalize(UnsafeNumericCast<uint32_t>(written), capacity);
			                          }
		                          });
		encode_all.GetSignature().GetParameter(0).SetName(Identifier("string"));
		encode_all.SetFallible();
		encode_all.GetProperties().SetProperty(MINIMUM_INPUT_WINDOW_PROPERTY,
		                                       Value::BIGINT(NumericCast<int64_t>(minimum_input_window)));
		set.AddFunction(std::move(encode_all));
	}
	return set;
}

optional_ptr<ScalarFunctionCatalogEntry> EncodingFunction::Find(ClientContext &context, const string &encoding_name) {
	// look in the encodings schema directly: the generic lookup would autoload extensions by function name
	auto lookup = [&]() -> optional_ptr<ScalarFunctionCatalogEntry> {
		auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
		auto &schema = Catalog::GetSystemCatalog(context).GetSchema(transaction, Identifier(SCHEMA));
		auto entry =
		    schema.GetEntry(transaction, CatalogType::SCALAR_FUNCTION_ENTRY, Identifier(FunctionName(encoding_name)));
		if (!entry) {
			return nullptr;
		}
		return &entry->Cast<ScalarFunctionCatalogEntry>();
	};
	auto entry = lookup();
	if (!entry && Catalog::TryAutoLoad(context, "encodings")) {
		entry = lookup();
	}
	return entry;
}

vector<string> EncodingFunction::GetEncodings(ClientContext &context) {
	vector<string> result {"utf-8"};
	auto &schema = Catalog::GetSystemCatalog(context).GetSchema(context, Identifier(SCHEMA));
	schema.Scan(context, CatalogType::SCALAR_FUNCTION_ENTRY, [&](CatalogEntry &entry) {
		if (entry.type == CatalogType::SCALAR_FUNCTION_ENTRY) {
			result.push_back(entry.name.GetIdentifierName());
		}
	});
	std::sort(result.begin(), result.end());
	return result;
}

unique_ptr<Expression> EncodingFunction::BindCall(ClientContext &context, vector<unique_ptr<Expression>> &children,
                                                  bool is_encode) {
	D_ASSERT(children.size() == 2);
	if (children[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!children[1]->IsFoldable()) {
		if (is_encode) {
			throw BinderException("encode: the encoding must be a constant");
		}
		return nullptr;
	}
	auto value = ExpressionExecutor::EvaluateScalar(context, *children[1]);
	if (value.IsNull()) {
		if (is_encode) {
			return make_uniq<BoundConstantExpression>(Value(LogicalType::BLOB));
		}
		return nullptr;
	}
	auto encoding_name = StringUtil::Lower(value.ToString());
	if (!is_encode && (encoding_name == "strict" || encoding_name == "replace" || encoding_name == "ignore")) {
		// the error option of decode
		return nullptr;
	}
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(std::move(children[0]));
	FunctionBinder binder(context);
	if (encoding_name == "utf-8") {
		return binder.BindScalarFunction(Identifier::DefaultSchema(), Identifier(is_encode ? "encode" : "decode"),
		                                 std::move(arguments));
	}
	auto entry = Find(context, encoding_name);
	if (!entry) {
		string error =
		    StringUtil::Format("Unknown encoding \"%s\". The currently supported encodings are:", value.ToString());
		for (auto &name : GetEncodings(context)) {
			error += "\n*  " + name;
		}
		throw BinderException(error);
	}
	ErrorData error;
	auto result = binder.BindScalarFunction(*entry, std::move(arguments), error);
	if (!result) {
		error.Throw();
	}
	return result;
}

} // namespace duckdb
