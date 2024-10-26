#pragma once

#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/string_map_set.hpp"

namespace duckdb {

template <class T>
struct EnumTypeInfoTemplated : public EnumTypeInfo {
	explicit EnumTypeInfoTemplated(Vector &values_insert_order_p, idx_t size_p)
	    : EnumTypeInfo(values_insert_order_p, size_p) {
		D_ASSERT(values_insert_order_p.GetType().InternalType() == PhysicalType::VARCHAR);

		UnifiedVectorFormat vdata;
		values_insert_order.ToUnifiedFormat(size_p, vdata);

		auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
		for (idx_t i = 0; i < size_p; i++) {
			auto idx = vdata.sel->get_index(i);
			if (!vdata.validity.RowIsValid(idx)) {
				throw InternalException("Attempted to create ENUM type with NULL value");
			}
			if (values.count(data[idx]) > 0) {
				throw InvalidInputException("Attempted to create ENUM type with duplicate value %s",
				                            data[idx].GetString());
			}
			values[data[idx]] = UnsafeNumericCast<T>(i);
		}
	}

	static shared_ptr<EnumTypeInfoTemplated> Deserialize(Deserializer &deserializer, uint32_t size) {
		Vector values_insert_order(LogicalType::VARCHAR, size);
		auto strings = FlatVector::GetData<string_t>(values_insert_order);

		deserializer.ReadList(201, "values", [&](Deserializer::List &list, idx_t i) {
			strings[i] = StringVector::AddStringOrBlob(values_insert_order, list.ReadElement<string>());
		});
		return make_shared_ptr<EnumTypeInfoTemplated>(values_insert_order, size);
	}

	const string_map_t<T> &GetValues() const {
		return values;
	}

	EnumTypeInfoTemplated(const EnumTypeInfoTemplated &) = delete;
	EnumTypeInfoTemplated &operator=(const EnumTypeInfoTemplated &) = delete;

private:
	string_map_t<T> values;
};

} // namespace duckdb
