#include "catch.hpp"

#include "duckdb/optimizer/column_binding_replacer.hpp"

using namespace duckdb;

TEST_CASE("Binding replacements stop at operator output boundaries", "[optimizer][bindings]") {
	auto binding_a = ColumnBinding(TableIndex(0), ProjectionIndex(0));
	auto binding_b = ColumnBinding(TableIndex(1), ProjectionIndex(0));
	auto binding_c = ColumnBinding(TableIndex(2), ProjectionIndex(0));

	BindingReplacementMap replacements;
	replacements.Add(binding_a, binding_b);
	replacements.Add(binding_b, binding_c);

	auto scoped = ColumnBindingRewrite::ScopeToOutput({binding_a}, {binding_b}, replacements);
	REQUIRE(scoped.Resolve(binding_a) == binding_b);
	REQUIRE(scoped.Resolve(binding_b) == binding_b);

	scoped = ColumnBindingRewrite::ScopeToOutput({binding_b}, {binding_b}, replacements);
	REQUIRE(scoped.Empty());

	scoped = ColumnBindingRewrite::PropagateOutput({binding_a}, {binding_b}, replacements);
	REQUIRE(scoped.Resolve(binding_a) == binding_b);
}
