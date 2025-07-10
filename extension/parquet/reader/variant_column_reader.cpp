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
	if (pending_skips > 0) {
		throw InternalException("VariantColumnReader cannot have pending skips");
	}
	optional_ptr<ColumnReader> typed_value_reader = child_readers.size() == 3 ? child_readers[2].get() : nullptr;

	// If the child reader values are all valid, "define_out" may not be initialized at all
	// So, we just initialize them to all be valid beforehand
	std::fill_n(define_out, num_values, MaxDefine());

	optional_idx read_count;
	Vector value_intermediate(LogicalType::BLOB, num_values);
	Vector metadata_intermediate(LogicalType::BLOB, num_values);
	unique_ptr<Vector> typed_value_intermediate;
	if (typed_value_reader) {
		typed_value_intermediate = make_uniq<Vector>(typed_value_reader->Type(), num_values);
	}

	auto metadata_values = child_readers[0]->Read(num_values, define_out, repeat_out, metadata_intermediate);
	auto value_values = child_readers[1]->Read(num_values, define_out, repeat_out, value_intermediate);
	idx_t typed_value_values = 0;
	if (typed_value_reader) {
		typed_value_values = typed_value_reader->Read(num_values, define_out, repeat_out, *typed_value_intermediate);
	}
	if (metadata_values != value_values) {
		throw InvalidInputException(
		    "The unshredded Variant column did not contain the same amount of values for 'metadata' and 'value'");
	}

	VariantBinaryDecoder decoder(context);

	auto result_data = FlatVector::GetData<string_t>(result);
	auto metadata_intermediate_data = FlatVector::GetData<string_t>(metadata_intermediate);
	auto value_intermediate_data = FlatVector::GetData<string_t>(value_intermediate);

	auto &result_validity = FlatVector::Validity(result);
	auto &metadata_validity = FlatVector::Validity(metadata_intermediate);
	auto &value_validity = FlatVector::Validity(value_intermediate);
	optional_ptr<ValidityMask> typed_value_validity;
	if (typed_value_reader) {
		typed_value_validity = FlatVector::Validity(*typed_value_intermediate);
	}
	for (idx_t i = 0; i < num_values; i++) {
		if (!metadata_validity.RowIsValid(i)) {
			throw InvalidInputException("The Variant 'metadata' can not be NULL");
		}
		VariantMetadata variant_metadata(metadata_intermediate_data[i]);
		VariantDecodeResult decode_result;
		decode_result.doc = yyjson_mut_doc_new(nullptr);

		VariantValue val;
		if (typed_value_validity && typed_value_validity->RowIsValid(i)) {
			//! This row has a typed value, the variant is (potentially partially) shredded on this type.
		} else if (value_validity.RowIsValid(i)) {
			auto value_data = reinterpret_cast<const_data_ptr_t>(value_intermediate_data[i].GetData());
			//! TODO: Do we want to create an intermediate representation, probably a Value ?
			//! Since we need to union the shredded value and unshredded values
			//! it's probably necessary to do this
			val = decoder.Decode(variant_metadata, value_data);
		} else {
			//! Missing from both 'typed_value' and 'value', emit null
			result_validity.SetInvalid(i);
		}

		//! Write the result to a string
		size_t len;
		auto json_val = val.ToJSON(decode_result.doc);
		decode_result.data =
		    yyjson_mut_val_write_opts(json_val, YYJSON_WRITE_ALLOW_INF_AND_NAN, nullptr, &len, nullptr);
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
