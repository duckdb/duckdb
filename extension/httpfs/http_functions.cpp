#include "duckdb/common/file_system.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "httpfs.hpp"
#include "http_functions.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// HTTPGetRequest
//------------------------------------------------------------------------------

static void HTTPGetRequestFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);

	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input) {
		auto file_system = make_uniq<HTTPFileSystem>();

		HTTPParams http_params = HTTPParams::ReadFrom(nullptr);
		http_params.force_download = true;
		http_params.keep_alive = false;

		std::string url = input.GetString();
		auto file_handle = make_uniq<HTTPFileHandle>(*file_system.get(), url, 0, http_params);
		file_handle->Initialize(nullptr);

		auto result_data = file_handle->cached_file_handle->GetData();
		auto result_size = file_handle->cached_file_handle->GetSize();
		return StringVector::AddString(result, result_data, result_size);
	});
}

//------------------------------------------------------------------------------
// HTTPPostRequest
//------------------------------------------------------------------------------

static void HTTPPostRequestFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 3);

	using STRING_TYPE = PrimitiveType<string_t>;
	using MAP_TYPE = PrimitiveType<list_entry_t>;

	auto &p1 = args.data[0];
	auto &p2 = args.data[1];
	auto &p2_entry = ListVector::GetEntry(p2);
	auto &p3 = args.data[2];

	GenericExecutor::ExecuteTernary<STRING_TYPE, MAP_TYPE, STRING_TYPE, STRING_TYPE>(
	    p1, p2, p3, result, args.size(), [&](STRING_TYPE p1, MAP_TYPE p2_offlen_, STRING_TYPE p3) {
		    auto file_system = make_uniq<HTTPFileSystem>();

		    HTTPParams http_params = HTTPParams::ReadFrom(nullptr);
		    http_params.force_download = false;
		    http_params.keep_alive = false;

		    auto input = p1.val;
		    auto p2_offlen = p2_offlen_.val;
		    auto input_body = p3.val;

		    HeaderMap header_map = {};

		    for (idx_t i = p2_offlen.offset; i < p2_offlen.offset + p2_offlen.length; i++) {
			    const auto &child_value = p2_entry.GetValue(i);

			    Vector tmp(child_value);
			    auto &children = StructVector::GetEntries(tmp);

			    if (children.size() == 2) {
				    auto name = FlatVector::GetData<string_t>(*children[0]);
				    auto data = FlatVector::GetData<string_t>(*children[1]);
				    header_map[name->GetString()] = data->GetString();
			    }
		    }

		    std::string url = input.GetString();
		    auto file_handle = make_uniq<HTTPFileHandle>(*file_system.get(), url, 0, http_params);

		    idx_t buffer_size = 2048;
		    auto buffer = duckdb::unique_ptr<char[]> {new char[buffer_size]};
		    std::string body = input_body.GetString();

		    auto response = file_system->PostRequest(*file_handle.get(), url, header_map, buffer, buffer_size,
		                                             (char *)body.c_str(), body.length());

		    auto result_data = buffer.get();
		    auto result_size = strlen((const char *)result_data);
		    return StringVector::AddString(result, result_data, result_size);
	    });
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------

void HTTPFunctions::RegisterHTTPRequestFunction(DatabaseInstance &db) {

	ScalarFunctionSet set01("HTTPGetRequest");
	set01.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, HTTPGetRequestFunction));

	ExtensionUtil::RegisterFunction(db, set01);

	ScalarFunctionSet set02("HTTPPostRequest");
	set02.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR), LogicalType::JSON()},
	    LogicalType::VARCHAR, HTTPPostRequestFunction));

	ExtensionUtil::RegisterFunction(db, set02);
}

} // namespace duckdb
