#include "reader/variant_column_reader.hpp"
#include "reader/variant/variant_binary_decoder.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Variant Column Reader
//===--------------------------------------------------------------------===//
VariantColumnReader::VariantColumnReader(ClientContext &context, ParquetReader &reader,
                                         const ParquetColumnSchema &schema,
                                         vector<unique_ptr<ColumnReader>> child_readers_p)
    : ColumnReader(reader, schema), context(context), child_readers(std::move(child_readers_p)) {
	D_ASSERT(Type().InternalType() == PhysicalType::VARCHAR);
}

ColumnReader &VariantColumnReader::GetChildReader(idx_t child_idx) {
	if (!child_readers[child_idx]) {
		throw InternalException("VariantColumnReader::GetChildReader(%d) - but this child reader is not set",
		                        child_idx);
	}
	return *child_readers[child_idx].get();
}

void VariantColumnReader::InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns,
                                         TProtocol &protocol_p) {
	for (auto &child : child_readers) {
		if (!child) {
			continue;
		}
		child->InitializeRead(row_group_idx_p, columns, protocol_p);
	}
}

idx_t VariantColumnReader::Read(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result) {
	if (child_readers.size() != 2) {
		throw NotImplementedException("Shredded Variant columns are not supported yet");
	}

	if (pending_skips > 0) {
		throw InternalException("VariantColumnReader cannot have pending skips");
	}

	// If the child reader values are all valid, "define_out" may not be initialized at all
	// So, we just initialize them to all be valid beforehand
	std::fill_n(define_out, num_values, MaxDefine());

	optional_idx read_count;
	Vector value_intermediate(LogicalType::BLOB, num_values);
	Vector metadata_intermediate(LogicalType::BLOB, num_values);
	auto metadata_values = child_readers[0]->Read(num_values, define_out, repeat_out, metadata_intermediate);
	auto value_values = child_readers[1]->Read(num_values, define_out, repeat_out, value_intermediate);
	if (metadata_values != value_values) {
		throw InvalidInputException(
		    "The unshredded Variant column did not contain the same amount of values for 'metadata' and 'value'");
	}

	VariantBinaryDecoder decoder(context);

	auto result_data = FlatVector::GetData<string_t>(result);
	auto metadata_intermediate_data = FlatVector::GetData<string_t>(metadata_intermediate);
	auto value_intermediate_data = FlatVector::GetData<string_t>(value_intermediate);

	auto metadata_validity = FlatVector::Validity(metadata_intermediate);
	auto value_validity = FlatVector::Validity(value_intermediate);
	for (idx_t i = 0; i < num_values; i++) {
		if (!metadata_validity.RowIsValid(i) || !value_validity.RowIsValid(i)) {
			throw InvalidInputException("The Variant 'metadata' and 'value' columns can not produce NULL values");
		}
		VariantMetadata variant_metadata(metadata_intermediate_data[i]);
		auto value_data = reinterpret_cast<const_data_ptr_t>(value_intermediate_data[i].GetData());

		VariantDecodeResult decode_result;
		decode_result.doc = yyjson_mut_doc_new(nullptr);

		auto val = decoder.Decode(decode_result.doc, variant_metadata, value_data);

		//! Write the result to a string
		size_t len;
		decode_result.data = yyjson_mut_val_write_opts(val, YYJSON_WRITE_ALLOW_INF_AND_NAN, nullptr, &len, nullptr);
		if (!decode_result.data) {
			throw InvalidInputException("Could not serialize the JSON to string, yyjson failed");
		}
		result_data[i] = StringVector::AddString(result, decode_result.data, static_cast<idx_t>(len));
	}

	read_count = value_values;
	return read_count.GetIndex();
}

void VariantColumnReader::Skip(idx_t num_values) {
	for (auto &child : child_readers) {
		if (!child) {
			continue;
		}
		child->Skip(num_values);
	}
}

void VariantColumnReader::RegisterPrefetch(ThriftFileTransport &transport, bool allow_merge) {
	for (auto &child : child_readers) {
		if (!child) {
			continue;
		}
		child->RegisterPrefetch(transport, allow_merge);
	}
}

uint64_t VariantColumnReader::TotalCompressedSize() {
	uint64_t size = 0;
	for (auto &child : child_readers) {
		if (!child) {
			continue;
		}
		size += child->TotalCompressedSize();
	}
	return size;
}

idx_t VariantColumnReader::GroupRowsAvailable() {
	for (auto &child : child_readers) {
		if (!child) {
			continue;
		}
		return child->GroupRowsAvailable();
	}
	throw InternalException("No projected columns in struct?");
}

} // namespace duckdb
