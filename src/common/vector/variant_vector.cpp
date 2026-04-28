#include "duckdb/common/vector/variant_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

namespace duckdb {

Vector &VariantVector::GetKeys(Vector &vec) {
	return StructVector::GetEntries(vec)[0];
}
const Vector &VariantVector::GetKeys(const Vector &vec) {
	return StructVector::GetEntries(vec)[0];
}

Vector &VariantVector::GetChildren(Vector &vec) {
	return StructVector::GetEntries(vec)[1];
}
const Vector &VariantVector::GetChildren(const Vector &vec) {
	return StructVector::GetEntries(vec)[1];
}

Vector &VariantVector::GetChildrenKeysIndex(Vector &vec) {
	auto &children = ListVector::GetChildMutable(GetChildren(vec));
	return StructVector::GetEntries(children)[0];
}

const Vector &VariantVector::GetChildrenKeysIndex(const Vector &vec) {
	auto &children = ListVector::GetChild(GetChildren(vec));
	return StructVector::GetEntries(children)[0];
}

Vector &VariantVector::GetChildrenValuesIndex(Vector &vec) {
	auto &children = ListVector::GetChildMutable(GetChildren(vec));
	return StructVector::GetEntries(children)[1];
}
const Vector &VariantVector::GetChildrenValuesIndex(const Vector &vec) {
	auto &children = ListVector::GetChild(GetChildren(vec));
	return StructVector::GetEntries(children)[1];
}

Vector &VariantVector::GetValues(Vector &vec) {
	return StructVector::GetEntries(vec)[2];
}
const Vector &VariantVector::GetValues(const Vector &vec) {
	return StructVector::GetEntries(vec)[2];
}

Vector &VariantVector::GetValuesTypeId(Vector &vec) {
	auto &values = ListVector::GetChildMutable(GetValues(vec));
	return StructVector::GetEntries(values)[0];
}
const Vector &VariantVector::GetValuesTypeId(const Vector &vec) {
	auto &values = ListVector::GetChild(GetValues(vec));
	return StructVector::GetEntries(values)[0];
}

Vector &VariantVector::GetValuesByteOffset(Vector &vec) {
	auto &values = ListVector::GetChildMutable(GetValues(vec));
	return StructVector::GetEntries(values)[1];
}
const Vector &VariantVector::GetValuesByteOffset(const Vector &vec) {
	auto &values = ListVector::GetChild(GetValues(vec));
	return StructVector::GetEntries(values)[1];
}

Vector &VariantVector::GetData(Vector &vec) {
	return StructVector::GetEntries(vec)[3];
}
const Vector &VariantVector::GetData(const Vector &vec) {
	return StructVector::GetEntries(vec)[3];
}

const UnifiedVectorFormat &UnifiedVariantVector::GetKeys(const RecursiveUnifiedVectorFormat &vec) {
	return vec.children[0].unified;
}

const UnifiedVectorFormat &UnifiedVariantVector::GetKeysEntry(const RecursiveUnifiedVectorFormat &vec) {
	return vec.children[0].children[0].unified;
}

const UnifiedVectorFormat &UnifiedVariantVector::GetChildren(const RecursiveUnifiedVectorFormat &vec) {
	return vec.children[1].unified;
}

const UnifiedVectorFormat &UnifiedVariantVector::GetChildrenKeysIndex(const RecursiveUnifiedVectorFormat &vec) {
	return vec.children[1].children[0].children[0].unified;
}

const UnifiedVectorFormat &UnifiedVariantVector::GetChildrenValuesIndex(const RecursiveUnifiedVectorFormat &vec) {
	return vec.children[1].children[0].children[1].unified;
}

const UnifiedVectorFormat &UnifiedVariantVector::GetValues(const RecursiveUnifiedVectorFormat &vec) {
	return vec.children[2].unified;
}

const UnifiedVectorFormat &UnifiedVariantVector::GetValuesTypeId(const RecursiveUnifiedVectorFormat &vec) {
	auto &values = vec.children[2];
	return values.children[0].children[0].unified;
}

const UnifiedVectorFormat &UnifiedVariantVector::GetValuesByteOffset(const RecursiveUnifiedVectorFormat &vec) {
	auto &values = vec.children[2];
	return values.children[0].children[1].unified;
}

const UnifiedVectorFormat &UnifiedVariantVector::GetData(const RecursiveUnifiedVectorFormat &vec) {
	return vec.children[3].unified;
}

} // namespace duckdb
