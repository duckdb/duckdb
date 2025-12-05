#include "reader/variant_column_reader.hpp"
#include "reader/variant/variant_binary_decoder.hpp"
#include "reader/variant/variant_shredded_conversion.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Variant Column Reader
//===--------------------------------------------------------------------===//
VariantColumnReader::VariantColumnReader(ClientContext &context, ParquetReader &reader,
                                         const ParquetColumnSchema &schema,
                                         vector<unique_ptr<ColumnReader>> child_readers_p)
    : ColumnReader(reader, schema), context(context), child_readers(std::move(child_readers_p)) {
	D_ASSERT(Type().InternalType() == PhysicalType::STRUCT);

	if (child_readers[0]->Schema().name == "metadata" && child_readers[1]->Schema().name == "value") {
		metadata_reader_idx = 0;
		value_reader_idx = 1;
	} else if (child_readers[1]->Schema().name == "metadata" && child_readers[0]->Schema().name == "value") {
		metadata_reader_idx = 1;
		value_reader_idx = 0;
	} else {
		throw InternalException("The Variant column must have 'metadata' and 'value' as the first two columns");
	}
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

static LogicalType GetIntermediateGroupType(optional_ptr<ColumnReader> typed_value) {
	child_list_t<LogicalType> children;
	children.emplace_back("value", LogicalType::BLOB);
	if (typed_value) {
		children.emplace_back("typed_value", typed_value->Type());
	}
	return LogicalType::STRUCT(std::move(children));
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

	Vector metadata_intermediate(LogicalType::BLOB, num_values);
	Vector intermediate_group(GetIntermediateGroupType(typed_value_reader), num_values);
	auto &group_entries = StructVector::GetEntries(intermediate_group);
	auto &value_intermediate = *group_entries[0];

	auto metadata_values =
	    child_readers[metadata_reader_idx]->Read(num_values, define_out, repeat_out, metadata_intermediate);
	auto value_values = child_readers[value_reader_idx]->Read(num_values, define_out, repeat_out, value_intermediate);

	D_ASSERT(child_readers[metadata_reader_idx]->Schema().name == "metadata");
	D_ASSERT(child_readers[value_reader_idx]->Schema().name == "value");

	if (metadata_values != value_values) {
		throw InvalidInputException(
		    "The Variant column did not contain the same amount of values for 'metadata' and 'value'");
	}

	vector<VariantValue> intermediate;
	if (typed_value_reader) {
		auto typed_values = typed_value_reader->Read(num_values, define_out, repeat_out, *group_entries[1]);
		if (typed_values != value_values) {
			throw InvalidInputException(
			    "The shredded Variant column did not contain the same amount of values for 'typed_value' and 'value'");
		}
	}
	intermediate =
	    VariantShreddedConversion::Convert(metadata_intermediate, intermediate_group, 0, num_values, num_values, false);
	VariantValue::ToVARIANT(intermediate, result);

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
