#include "catch.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"

using namespace duckdb;

TEST_CASE("Test that LogicalType::WithAlias does not modify shared type info", "[logical_type_immutable]") {
	SECTION("nested types share their extra type info") {
		auto base = LogicalType::LIST(LogicalType::INTEGER);
		auto shared = base;

		auto aliased = base.WithAlias("MY_LIST");
		REQUIRE(!base.HasAlias());
		REQUIRE(!shared.HasAlias());
		REQUIRE(aliased.GetAlias() == "MY_LIST");
		REQUIRE(aliased.InternalType() == base.InternalType());
		REQUIRE(ListType::GetChildType(aliased) == LogicalType::INTEGER);
	}

	SECTION("re-aliasing does not modify the previous alias") {
		auto first = LogicalType::LIST(LogicalType::INTEGER).WithAlias("FIRST");
		auto second = first.WithAlias("SECOND");
		REQUIRE(first.GetAlias() == "FIRST");
		REQUIRE(second.GetAlias() == "SECOND");
	}

	SECTION("enums are unshared and keep a working dictionary") {
		Vector values(LogicalType::VARCHAR, 2);
		auto data = FlatVector::GetDataMutable<string_t>(values);
		data[0] = StringVector::AddString(values, "a");
		data[1] = StringVector::AddString(values, "b");

		auto base = LogicalType::ENUM(values, 2);
		auto shared = base;
		auto aliased = base.WithAlias("MOOD");

		REQUIRE(!base.HasAlias());
		REQUIRE(!shared.HasAlias());
		REQUIRE(aliased.GetAlias() == "MOOD");
		REQUIRE(EnumType::GetSize(aliased) == 2);
		REQUIRE(EnumType::GetPos(aliased, string_t("b")) == 1);
		REQUIRE(EnumType::GetPos(aliased, string_t("c")) == -1);
	}

	SECTION("an empty alias does not allocate extra type info") {
		auto type = LogicalType(LogicalTypeId::INTEGER);
		REQUIRE(!type.WithAlias("").AuxInfo());
		REQUIRE(type.WithAlias("") == type);
		// a generic type info with an empty alias compares equal to a type without type info
		REQUIRE(type.WithAlias("x").WithAlias("") == type);
	}
}

TEST_CASE("Test that LogicalType::WithExtensionInfo does not modify shared type info", "[logical_type_immutable]") {
	auto base = LogicalType::LIST(LogicalType::INTEGER);
	auto shared = base;

	auto info = make_uniq<ExtensionTypeInfo>();
	info->modifiers.emplace_back(Value::INTEGER(42));
	auto extended = base.WithExtensionInfo(std::move(info));

	REQUIRE(!base.HasExtensionInfo());
	REQUIRE(!shared.HasExtensionInfo());
	REQUIRE(extended.HasExtensionInfo());
	REQUIRE(extended.GetExtensionInfo()->modifiers[0].value.GetValue<int32_t>() == 42);
}
