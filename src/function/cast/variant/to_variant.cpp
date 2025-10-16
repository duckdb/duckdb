#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"

#include "duckdb/function/cast/variant/to_variant.hpp"

namespace duckdb {
namespace variant {

static void InitializeOffsets(DataChunk &offsets, idx_t count) {
	auto keys = OffsetData::GetKeys(offsets);
	auto children = OffsetData::GetChildren(offsets);
	auto values = OffsetData::GetValues(offsets);
	auto blob = OffsetData::GetBlob(offsets);
	for (idx_t i = 0; i < count; i++) {
		keys[i] = 0;
		children[i] = 0;
		values[i] = 0;
		blob[i] = 0;
	}
}

static void InitializeVariants(DataChunk &offsets, Vector &result, SelectionVector &keys_selvec, idx_t &selvec_size,
                               OrderedOwningStringMap<uint32_t> &dictionary) {
	auto &keys = VariantVector::GetKeys(result);
	auto keys_data = ListVector::GetData(keys);

	auto &children = VariantVector::GetChildren(result);
	auto children_data = ListVector::GetData(children);

	auto &values = VariantVector::GetValues(result);
	auto values_data = ListVector::GetData(values);

	auto &blob = VariantVector::GetData(result);
	auto blob_data = FlatVector::GetData<string_t>(blob);

	idx_t keys_offset = 0;
	idx_t children_offset = 0;
	idx_t values_offset = 0;

	auto keys_sizes = OffsetData::GetKeys(offsets);
	auto children_sizes = OffsetData::GetChildren(offsets);
	auto values_sizes = OffsetData::GetValues(offsets);
	auto blob_sizes = OffsetData::GetBlob(offsets);
	for (idx_t i = 0; i < offsets.size(); i++) {
		auto &keys_entry = keys_data[i];
		auto &children_entry = children_data[i];
		auto &values_entry = values_data[i];

		//! keys
		keys_entry.length = keys_sizes[i];
		keys_entry.offset = keys_offset;
		keys_offset += keys_entry.length;

		//! children
		children_entry.length = children_sizes[i];
		children_entry.offset = children_offset;
		children_offset += children_entry.length;

		//! values
		values_entry.length = values_sizes[i];
		values_entry.offset = values_offset;
		values_offset += values_entry.length;

		//! value
		blob_data[i] = StringVector::EmptyString(blob, blob_sizes[i]);
	}

	//! Reserve for the children of the lists
	ListVector::Reserve(keys, keys_offset);
	ListVector::Reserve(children, children_offset);
	ListVector::Reserve(values, values_offset);

	//! Set list sizes
	ListVector::SetListSize(keys, keys_offset);
	ListVector::SetListSize(children, children_offset);
	ListVector::SetListSize(values, values_offset);

	keys_selvec.Initialize(keys_offset);
	selvec_size = keys_offset;
}

static bool GatherOffsetsAndSizes(ToVariantSourceData &source, ToVariantGlobalResultData &result, idx_t count) {
	InitializeOffsets(result.offsets, count);
	//! First pass - collect sizes/offsets
	return ConvertToVariant<false>(source, result, count, nullptr, nullptr, true);
}

static bool WriteVariantResultData(ToVariantSourceData &source, ToVariantGlobalResultData &result, idx_t count) {
	InitializeOffsets(result.offsets, count);
	//! Second pass - actually construct the variants
	return ConvertToVariant<true>(source, result, count, nullptr, nullptr, true);
}

static bool CastToVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	if (!count) {
		return true;
	}
	DataChunk offsets;
	offsets.Initialize(Allocator::DefaultAllocator(),
	                   {LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UINTEGER},
	                   count);
	offsets.SetCardinality(count);
	auto &keys = VariantVector::GetKeys(result);
	auto &keys_entry = ListVector::GetEntry(keys);

	//! Initialize the dictionary
	OrderedOwningStringMap<uint32_t> dictionary(StringVector::GetStringBuffer(keys_entry).GetStringAllocator());
	SelectionVector keys_selvec;
	ToVariantSourceData source_data(source, count);

	{
		VariantVectorData variant_data(result);
		ToVariantGlobalResultData result_data(variant_data, offsets, dictionary, keys_selvec);
		if (!GatherOffsetsAndSizes(source_data, result_data, count)) {
			return false;
		}
	}

	//! This resizes the lists, invalidating the "GetData" results stored in VariantVectorData
	idx_t keys_selvec_size;
	InitializeVariants(offsets, result, keys_selvec, keys_selvec_size, dictionary);

	{
		VariantVectorData variant_data(result);
		ToVariantGlobalResultData result_data(variant_data, offsets, dictionary, keys_selvec);
		if (!WriteVariantResultData(source_data, result_data, count)) {
			return false;
		}
	}

	VariantUtils::FinalizeVariantKeys(result, dictionary, keys_selvec, keys_selvec_size);
	//! Finalize the 'data'
	auto &blob = VariantVector::GetData(result);
	auto blob_data = FlatVector::GetData<string_t>(blob);
	auto blob_offsets = OffsetData::GetBlob(offsets);
	for (idx_t i = 0; i < count; i++) {
		auto size = blob_offsets[i];
		blob_data[i].SetSizeAndFinalize(size, size);
	}

	keys_entry.Slice(keys_selvec, keys_selvec_size);
	keys_entry.Flatten(keys_selvec_size);

	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
	return true;
}

} // namespace variant

BoundCastInfo DefaultCasts::ImplicitToVariantCast(BindCastInput &input, const LogicalType &source,
                                                  const LogicalType &target) {
	return BoundCastInfo(variant::CastToVARIANT);
}

} // namespace duckdb
