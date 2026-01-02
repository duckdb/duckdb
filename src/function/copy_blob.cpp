#include "duckdb/common/file_system.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

//----------------------------------------------------------------------------------------------------------------------
// Bind
//----------------------------------------------------------------------------------------------------------------------
namespace {

struct WriteBlobBindData final : public TableFunctionData {
	FileCompressionType compression_type = FileCompressionType::AUTO_DETECT;
};

string ParseStringOption(const Value &value, const string &loption) {
	if (value.IsNull()) {
		return string();
	}
	if (value.type().id() == LogicalTypeId::LIST) {
		auto &children = ListValue::GetChildren(value);
		if (children.size() != 1) {
			throw BinderException("\"%s\" expects a single argument as a string value", loption);
		}
		return ParseStringOption(children[0], loption);
	}
	if (value.type().id() != LogicalTypeId::VARCHAR) {
		throw BinderException("\"%s\" expects a string argument!", loption);
	}
	return value.GetValue<string>();
}

unique_ptr<FunctionData> WriteBlobBind(ClientContext &context, CopyFunctionBindInput &input,
                                       const vector<string> &names, const vector<LogicalType> &sql_types) {
	if (sql_types.size() != 1 || sql_types.back().id() != LogicalTypeId::BLOB) {
		throw BinderException("\"COPY (FORMAT BLOB)\" only supports a single BLOB column");
	}

	auto result = make_uniq<WriteBlobBindData>();

	for (auto &lopt : input.info.options) {
		if (StringUtil::CIEquals(lopt.first, "compression")) {
			auto compression_str = ParseStringOption(lopt.second[0], lopt.first);
			result->compression_type = FileCompressionTypeFromString(compression_str);
		} else {
			throw BinderException("Unrecognized option for COPY (FORMAT BLOB): \"%s\"", lopt.first);
		}
	}

	return std::move(result);
}

//----------------------------------------------------------------------------------------------------------------------
// Global State
//----------------------------------------------------------------------------------------------------------------------
struct WriteBlobGlobalState final : public GlobalFunctionData {
	unique_ptr<FileHandle> handle;
	mutex lock;
};

unique_ptr<GlobalFunctionData> WriteBlobInitializeGlobal(ClientContext &context, FunctionData &bind_data,
                                                         const string &file_path) {
	auto &bdata = bind_data.Cast<WriteBlobBindData>();
	auto &fs = FileSystem::GetFileSystem(context);

	auto flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW | bdata.compression_type;
	auto handle = fs.OpenFile(file_path, flags);

	auto result = make_uniq<WriteBlobGlobalState>();
	result->handle = std::move(handle);

	return std::move(result);
}

//----------------------------------------------------------------------------------------------------------------------
// Local State
//----------------------------------------------------------------------------------------------------------------------
struct WriteBlobLocalState final : public LocalFunctionData {};

unique_ptr<LocalFunctionData> WriteBlobInitializeLocal(ExecutionContext &context, FunctionData &bind_data) {
	return make_uniq_base<LocalFunctionData, WriteBlobLocalState>();
}

//----------------------------------------------------------------------------------------------------------------------
// Sink
//----------------------------------------------------------------------------------------------------------------------
void WriteBlobSink(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                   LocalFunctionData &lstate, DataChunk &input) {
	D_ASSERT(input.ColumnCount() == 1);

	auto &state = gstate.Cast<WriteBlobGlobalState>();
	lock_guard<mutex> glock(state.lock);

	auto &handle = state.handle;

	UnifiedVectorFormat vdata;
	input.data[0].ToUnifiedFormat(input.size(), vdata);
	const auto blobs = UnifiedVectorFormat::GetData<string_t>(vdata);

	for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
		const auto out_idx = vdata.sel->get_index(row_idx);
		if (vdata.validity.RowIsValid(out_idx)) {
			auto &blob = blobs[out_idx];
			auto blob_len = blob.GetSize();
			auto blob_ptr = blob.GetDataWriteable();
			auto blob_end = blob_ptr + blob_len;

			while (blob_ptr < blob_end) {
				auto written = handle->Write(blob_ptr, blob_len);
				if (written <= 0) {
					throw IOException("Failed to write to file!");
				}
				blob_ptr += written;
			}
		}
	}
}

//----------------------------------------------------------------------------------------------------------------------
// Combine
//----------------------------------------------------------------------------------------------------------------------
void WriteBlobCombine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                      LocalFunctionData &lstate) {
}

//----------------------------------------------------------------------------------------------------------------------
// Finalize
//----------------------------------------------------------------------------------------------------------------------
void WriteBlobFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &state = gstate.Cast<WriteBlobGlobalState>();
	lock_guard<mutex> glock(state.lock);

	state.handle->Close();
}

} // namespace

//----------------------------------------------------------------------------------------------------------------------
// Register
//----------------------------------------------------------------------------------------------------------------------
void BuiltinFunctions::RegisterCopyFunctions() {
	CopyFunction info("blob");
	info.copy_to_bind = WriteBlobBind;
	info.copy_to_initialize_local = WriteBlobInitializeLocal;
	info.copy_to_initialize_global = WriteBlobInitializeGlobal;
	info.copy_to_sink = WriteBlobSink;
	info.copy_to_combine = WriteBlobCombine;
	info.copy_to_finalize = WriteBlobFinalize;
	info.extension = "blob";

	AddFunction(info);
}

} // namespace duckdb
