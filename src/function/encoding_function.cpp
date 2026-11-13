#include "duckdb/function/encoding_function.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/function/built_in_functions.hpp"

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

ScalarFunctionSet EncodingFunction::Create(const string &encoding_name, decode_t decode, idx_t max_output_bytes) {
	D_ASSERT(decode && max_output_bytes > 0);
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
	decode_all.SetFallible();

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
	decode_stream.SetFallible();

	ScalarFunctionSet set(name);
	set.AddFunction(std::move(decode_all));
	set.AddFunction(std::move(decode_stream));
	return set;
}

} // namespace duckdb
