#include "duckdb/execution/operator/csv_scanner/encode/csv_encoder.hpp"
#include "duckdb/catalog/catalog.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/function/encoding_function.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

void CSVEncoderBuffer::Initialize(idx_t encoded_size) {
	encoded_buffer_size = encoded_size;
	encoded_buffer = duckdb::unique_ptr<char[]>(new char[encoded_size]);
}

char *CSVEncoderBuffer::Ptr() const {
	return encoded_buffer.get();
}

idx_t CSVEncoderBuffer::GetCapacity() const {
	return encoded_buffer_size;
}

idx_t CSVEncoderBuffer::GetSize() const {
	return actual_encoded_buffer_size;
}

void CSVEncoderBuffer::SetSize(const idx_t buffer_size) {
	D_ASSERT(buffer_size <= encoded_buffer_size);
	actual_encoded_buffer_size = buffer_size;
}

bool CSVEncoderBuffer::HasDataToRead() const {
	return cur_pos < actual_encoded_buffer_size;
}

void CSVEncoderBuffer::Reset() {
	cur_pos = 0;
	actual_encoded_buffer_size = 0;
}

static optional_ptr<ScalarFunctionCatalogEntry> FindEncoding(ClientContext &context, const string &encoding_name) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	return catalog.GetEntry<ScalarFunctionCatalogEntry>(
	    context,
	    QualifiedName(catalog.GetName(), Identifier::DefaultSchema(),
	                  Identifier(EncodingFunction::FunctionName(encoding_name))),
	    OnEntryNotFound::RETURN_NULL);
}

//! The names of all encodings: scalar functions that carry encoding info
static vector<string> GetLoadedEncodings(ClientContext &context) {
	vector<string> result {"utf-8"};
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &schema = catalog.GetSchema(context, Identifier::DefaultSchema());
	schema.Scan(context, CatalogType::SCALAR_FUNCTION_ENTRY, [&](CatalogEntry &entry) {
		if (entry.type != CatalogType::SCALAR_FUNCTION_ENTRY) {
			return;
		}
		auto &functions = entry.Cast<ScalarFunctionCatalogEntry>().functions;
		for (idx_t i = 0; i < functions.Size(); i++) {
			auto &function = functions.GetFunctionByOffset(i);
			if (function->HasExtraFunctionInfo()) {
				auto info = dynamic_cast<EncodingFunctionInfo *>(&function->GetExtraFunctionInfo());
				if (info) {
					result.push_back(info->encoding_name);
					break;
				}
			}
		}
	});
	std::sort(result.begin(), result.end());
	return result;
}

CSVEncoder::CSVEncoder(ClientContext &context_p, const string &encoding_name_to_find, idx_t buffer_size)
    : context(context_p), pass_on_byte(0) {
	encoding_name = StringUtil::Lower(encoding_name_to_find);
	if (encoding_name == "utf-8") {
		// UTF-8 is read straight into the CSV buffers (see CSVFileHandle::Read), the encoder buffers are never used
		return;
	}
	auto entry = FindEncoding(context_p, encoding_name);
	if (!entry) {
		// Maybe we can try to auto-load from our encodings extension, if this somehow fails, we just error.
		if (Catalog::TryAutoLoad(context_p, "encodings")) {
			entry = FindEncoding(context_p, encoding_name);
		}
	}
	if (!entry) {
		// If at this point we still do not have a function we throw an error.
		std::ostringstream error;
		error << "The CSV Reader does not support the encoding: \"" << encoding_name_to_find << "\"\n";
		if (!context_p.db->ExtensionIsLoaded("encodings")) {
			error << "It is possible that the encoding exists in the encodings extension. You can try \"INSTALL "
			         "encodings; LOAD encodings\""
			      << "\n";
		}
		error << "The currently supported encodings are: " << '\n';
		for (auto &name : GetLoadedEncodings(context_p)) {
			error << "*  " << name << '\n';
		}
		throw InvalidInputException(error.str());
	}

	// Bind the streaming overload: decode_<name>(encoded BLOB, is_last BOOLEAN)
	auto function = entry->functions.GetFunctionByArguments(context_p, {LogicalType::BLOB, LogicalType::BOOLEAN});
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BLOB, 0));
	children.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BOOLEAN, 1));
	decode_expression = function->Bind(context_p, std::move(children));
	decode_executor = make_uniq<ExpressionExecutor>(context_p, *decode_expression);
	decode_input.Initialize(context_p, {LogicalType::BLOB, LogicalType::BOOLEAN}, 1);
	auto &info = function->GetExtraFunctionInfo().Cast<EncodingFunctionInfo>();
	lookup_bytes = info.lookup_bytes;
	max_output_bytes = info.max_output_bytes;

	// The encoded buffer is even for the two byte lookup of utf-16, and fits a full lookup plus the probe byte
	encoded_buffer.Initialize(MaxValue<idx_t>(buffer_size - buffer_size % 2, 2 * lookup_bytes + 2));
}

void CSVEncoder::Reset() {
	encoded_buffer.Reset();
	encoded_buffer.last_buffer = false;
	pending_decoded.clear();
	pending_pos = 0;
	has_pass_on_byte = false;
	encoded_bytes_read = 0;
}

idx_t CSVEncoder::Decode(const char *input, idx_t input_size, bool is_last, string &decoded) {
	decode_input.Reset();
	FlatVector::GetDataMutable<string_t>(decode_input.data[0])[0] =
	    string_t(input, UnsafeNumericCast<uint32_t>(input_size));
	FlatVector::GetDataMutable<bool>(decode_input.data[1])[0] = is_last;
	decode_input.SetCardinality(1);
	Vector result(EncodingFunction::StreamResultType(), 1);
	decode_executor->ExecuteExpression(decode_input, result);
	auto values = result.Values<VectorStructType<string_t, int64_t>>();
	auto entry = values[0];
	auto decoded_value = entry.GetChildValue<0>().GetValue();
	decoded.assign(decoded_value.GetData(), decoded_value.GetSize());
	return UnsafeNumericCast<idx_t>(entry.GetChildValue<1>().GetValue());
}

idx_t CSVEncoder::DecodeBuffer(char *output_buffer, idx_t output_pos, idx_t output_size) {
	string decoded;
	while (encoded_buffer.HasDataToRead() && output_pos < output_size) {
		const idx_t available = encoded_buffer.GetSize() - encoded_buffer.cur_pos;
		// Decode as much as is guaranteed to fit in the output, but always enough to complete one character
		idx_t slice = MaxValue<idx_t>((output_size - output_pos) / max_output_bytes, lookup_bytes);
		slice = MinValue<idx_t>(slice, available);
		const bool is_last = encoded_buffer.last_buffer && slice == available;
		const idx_t consumed = Decode(encoded_buffer.Ptr() + encoded_buffer.cur_pos, slice, is_last, decoded);
		if (consumed == 0) {
			// An incomplete character at the end of the buffer
			break;
		}
		encoded_buffer.cur_pos += consumed;
		const idx_t to_copy = MinValue<idx_t>(decoded.size(), output_size - output_pos);
		memcpy(output_buffer + output_pos, decoded.data(), to_copy);
		output_pos += to_copy;
		if (to_copy < decoded.size()) {
			pending_decoded.assign(decoded, to_copy, string::npos);
			pending_pos = 0;
		}
	}
	return output_pos;
}

idx_t CSVEncoder::Encode(FileHandle &file_handle_input, char *output_buffer, const idx_t decoded_buffer_size) {
	idx_t output_buffer_pos = 0;
	// Check if we have some left-overs. These can either be
	// 1. decoded bytes that did not fit the previous output buffer
	if (pending_pos < pending_decoded.size()) {
		const idx_t to_copy = MinValue<idx_t>(pending_decoded.size() - pending_pos, decoded_buffer_size);
		memcpy(output_buffer, pending_decoded.data() + pending_pos, to_copy);
		pending_pos += to_copy;
		output_buffer_pos += to_copy;
	}
	// 2. remaining encoded buffer
	if (pending_pos == pending_decoded.size()) {
		output_buffer_pos = DecodeBuffer(output_buffer, output_buffer_pos, decoded_buffer_size);
	}
	// 3. a new encoded buffer from the file
	while (output_buffer_pos < decoded_buffer_size) {
		idx_t current_decoded_buffer_start = output_buffer_pos;
		// An incomplete character at the end of the buffer is moved to the start of the next buffer
		const idx_t tail_size = encoded_buffer.GetSize() - encoded_buffer.cur_pos;
		memmove(encoded_buffer.Ptr(), encoded_buffer.Ptr() + encoded_buffer.cur_pos, tail_size);
		encoded_buffer.Reset();
		if (has_pass_on_byte) {
			encoded_buffer.Ptr()[tail_size] = pass_on_byte;
		}
		const idx_t bytes_to_read = encoded_buffer.GetCapacity() - tail_size - has_pass_on_byte;
		auto actual_encoded_bytes = static_cast<idx_t>(
		    file_handle_input.Read(context, encoded_buffer.Ptr() + tail_size + has_pass_on_byte, bytes_to_read));
		encoded_bytes_read += actual_encoded_bytes;
		encoded_buffer.SetSize(actual_encoded_bytes + tail_size + has_pass_on_byte);
		if (actual_encoded_bytes < bytes_to_read) {
			encoded_buffer.last_buffer = true;
			has_pass_on_byte = false;
		} else {
			auto bytes_read = static_cast<idx_t>(file_handle_input.Read(context, &pass_on_byte, 1));
			encoded_bytes_read += bytes_read;
			if (bytes_read == 0) {
				encoded_buffer.last_buffer = true;
				has_pass_on_byte = false;
			} else {
				has_pass_on_byte = true;
			}
		}
		output_buffer_pos = DecodeBuffer(output_buffer, output_buffer_pos, decoded_buffer_size);
		if (output_buffer_pos == current_decoded_buffer_start) {
			return output_buffer_pos;
		}
	}
	return output_buffer_pos;
}

} // namespace duckdb
