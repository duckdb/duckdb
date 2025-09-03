#include "duckdb/common/file_system.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

//----------------------------------------------------------------------------------------------------------------------
// Bind
//----------------------------------------------------------------------------------------------------------------------
namespace {

struct WriteBlobBindData final : public TableFunctionData {};

unique_ptr<FunctionData> WriteBlobBind(ClientContext &context, CopyFunctionBindInput &input,
                                       const vector<string> &names, const vector<LogicalType> &sql_types) {
	if (sql_types.size() != 1 || sql_types.back().id() != LogicalTypeId::BLOB) {
		throw BinderException("\"COPY (FORMAT BLOB)\" only supports a single BLOB column");
	}

	return make_uniq_base<FunctionData, WriteBlobBindData>();
}

//----------------------------------------------------------------------------------------------------------------------
// Global State
//----------------------------------------------------------------------------------------------------------------------
struct WriteBlobGlobalState final : public GlobalFunctionData {
	unique_ptr<FileHandle> handle;
	mutex lock;
};

static unique_ptr<GlobalFunctionData> WriteBlobInitializeGlobal(ClientContext &context, FunctionData &bind_data,
                                                                const string &file_path) {

	auto &fs = FileSystem::GetFileSystem(context);
	auto handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);

	auto result = make_uniq<WriteBlobGlobalState>();
	result->handle = std::move(handle);

	return std::move(result);
}

//----------------------------------------------------------------------------------------------------------------------
// Local State
//----------------------------------------------------------------------------------------------------------------------
struct WriteBlobLocalState final : public LocalFunctionData {};

static unique_ptr<LocalFunctionData> WriteBlobInitializeLocal(ExecutionContext &context, FunctionData &bind_data) {
	return make_uniq_base<LocalFunctionData, WriteBlobLocalState>();
}

//----------------------------------------------------------------------------------------------------------------------
// Sink
//----------------------------------------------------------------------------------------------------------------------
static void WriteBlobSink(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
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
static void WriteBlobCombine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                             LocalFunctionData &lstate) {
}

//----------------------------------------------------------------------------------------------------------------------
// Finalize
//----------------------------------------------------------------------------------------------------------------------
void WriteBlobFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &state = gstate.Cast<WriteBlobGlobalState>();
	lock_guard<mutex> glock(state.lock);

	state.handle->Sync();
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
