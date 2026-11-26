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

CSVEncoder::CSVEncoder(ClientContext &context_p, const string &encoding_name_to_find, idx_t buffer_size)
    : context(context_p), pass_on_byte(0) {
	encoding_name = StringUtil::Lower(encoding_name_to_find);
	if (encoding_name == "utf-8") {
		// UTF-8 is read straight into the CSV buffers (see CSVFileHandle::Read), the encoder buffers are never used
		return;
	}
	auto entry = EncodingFunction::Find(context_p, encoding_name);
	if (!entry) {
		std::ostringstream error;
		error << "The CSV Reader does not support the encoding: \"" << encoding_name_to_find << "\"\n";
		if (!context_p.db->ExtensionIsLoaded("encodings")) {
			error << "It is possible that the encoding exists in the encodings extension. You can try \"INSTALL "
			         "encodings; LOAD encodings\""
			      << "\n";
		}
		error << "The currently supported encodings are: " << '\n';
		for (auto &name : EncodingFunction::GetEncodings(context_p)) {
			error << "*  " << name << '\n';
		}
		throw InvalidInputException(error.str());
	}

	auto function = BindDecoder(context_p, *entry);
	decode_input.Initialize(context_p, {LogicalType::BLOB, LogicalType::BOOLEAN}, 1);

	auto window = function->GetProperties().GetProperty(EncodingFunction::MINIMUM_INPUT_WINDOW_PROPERTY);
	if (window) {
		minimum_input_window = MaxValue<idx_t>(window->GetValue<idx_t>(), 1);
	}
	auto marks = function->GetProperties().GetProperty(EncodingFunction::BYTE_ORDER_MARKS_PROPERTY);
	if (marks) {
		for (auto &mark : ListValue::GetChildren(*marks)) {
			auto &fields = StructValue::GetChildren(mark);
			byte_order_marks.emplace_back(StringValue::Get(fields[0]), StringValue::Get(fields[1]));
		}
	}
	// The encoded buffer is even for utf-16, and fits a full input window plus the probe byte
	encoded_buffer.Initialize(MaxValue<idx_t>(buffer_size - buffer_size % 2, 2 * minimum_input_window + 2));
}

shared_ptr<const ScalarFunction> CSVEncoder::BindDecoder(ClientContext &context_p, ScalarFunctionCatalogEntry &entry) {
	// Bind the streaming overload: decode_<name>(encoded BLOB, is_last BOOLEAN)
	auto function = entry.functions.GetFunctionByArguments(context_p, {LogicalType::BLOB, LogicalType::BOOLEAN});
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BLOB, 0));
	children.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BOOLEAN, 1));
	decode_expression = function->Bind(context_p, std::move(children));
	decode_executor = make_uniq<ExpressionExecutor>(context_p, *decode_expression);
	return function;
}

void CSVEncoder::DetectByteOrder() {
	for (auto &mark : byte_order_marks) {
		if (encoded_buffer.GetSize() < mark.first.size() ||
		    memcmp(encoded_buffer.Ptr(), mark.first.data(), mark.first.size()) != 0) {
			continue;
		}
		auto &client_context = *context.GetClientContext();
		auto entry = EncodingFunction::Find(client_context, mark.second);
		if (!entry) {
			throw InternalException("Encoding \"%s\" picked by a byte order mark does not exist", mark.second);
		}
		BindDecoder(client_context, *entry);
		return;
	}
}

void CSVEncoder::Reset() {
	encoded_buffer.Reset();
	encoded_buffer.last_buffer = false;
	pending_decoded.clear();
	pending_pos = 0;
	has_pass_on_byte = false;
	encoded_bytes_read = 0;
}

idx_t CSVEncoder::Decode(const char *input, idx_t input_size, bool is_last, Vector &result, string_t &decoded) {
	decode_input.Reset();
	decode_input.SetChildCardinality(1);
	FlatVector::GetDataMutable<string_t>(decode_input.data[0])[0] =
	    string_t(input, UnsafeNumericCast<uint32_t>(input_size));
	FlatVector::GetDataMutable<bool>(decode_input.data[1])[0] = is_last;
	decode_executor->ExecuteExpression(decode_input, result);
	auto values = result.Values<VectorStructType<string_t, int64_t>>();
	auto entry = values[0];
	decoded = entry.GetChildValue<0>().GetValue();
	return UnsafeNumericCast<idx_t>(entry.GetChildValue<1>().GetValue());
}

idx_t CSVEncoder::DecodeBuffer(char *output_buffer, idx_t output_pos, idx_t output_size) {
	while (encoded_buffer.HasDataToRead() && output_pos < output_size) {
		const idx_t available = encoded_buffer.GetSize() - encoded_buffer.cur_pos;
		// Decode as much input as there is room in the output, extended until it holds a complete character
		const idx_t room = MinValue<idx_t>(output_size - output_pos, MAX_DECODE_SLICE);
		idx_t slice = MinValue<idx_t>(MaxValue<idx_t>(room, minimum_input_window), available);
		idx_t consumed = 0;
		Vector result(EncodingFunction::StreamResultType(), 1);
		string_t decoded;
		while (true) {
			const bool is_last = encoded_buffer.last_buffer && slice == available;
			consumed = Decode(encoded_buffer.Ptr() + encoded_buffer.cur_pos, slice, is_last, result, decoded);
			if (consumed != 0 || slice == available) {
				break;
			}
			slice = MinValue<idx_t>(slice * 2, available);
		}
		if (consumed == 0) {
			// An incomplete character at the end of the buffer
			break;
		}
		encoded_buffer.cur_pos += consumed;
		const idx_t to_copy = MinValue<idx_t>(decoded.GetSize(), output_size - output_pos);
		memcpy(output_buffer + output_pos, decoded.GetData(), to_copy); // NOLINT(bugprone-not-null-terminated-result)
		output_pos += to_copy;
		if (to_copy < decoded.GetSize()) {
			pending_decoded.assign(decoded.GetData() + to_copy, decoded.GetSize() - to_copy);
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
		const bool file_start = encoded_bytes_read == 0;
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
		if (file_start) {
			DetectByteOrder();
		}
		output_buffer_pos = DecodeBuffer(output_buffer, output_buffer_pos, decoded_buffer_size);
		if (output_buffer_pos == current_decoded_buffer_start) {
			return output_buffer_pos;
		}
	}
	return output_buffer_pos;
}

} // namespace duckdb
