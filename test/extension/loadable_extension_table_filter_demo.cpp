#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/table_filter_extension.hpp"
#include "duckdb/planner/filter/extension_table_filter.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

using namespace duckdb;

struct ModuloFilterState : public ExtensionFilterState {
	int modulus;
};

class ModuloFilter : public ExtensionTableFilter {
public:
	ModuloFilter(int modulus, int remainder) : modulus(modulus), remainder(remainder) {
	}

	int modulus;
	int remainder;

	string ToString(const string &column_name) const override {
		return column_name + " % " + std::to_string(modulus) + " = " + std::to_string(remainder);
	}

	duckdb::unique_ptr<TableFilter> Copy() const override {
		return make_uniq<ModuloFilter>(modulus, remainder);
	}

	void Serialize(Serializer &serializer) const override {
		TableFilter::Serialize(serializer);
		serializer.WriteProperty(200, "extension_name", string("modulo_filter"));
		serializer.WriteProperty(201, "modulus", modulus);
		serializer.WriteProperty(202, "remainder", remainder);
	}

	static duckdb::unique_ptr<TableFilter> Deserialize(Deserializer &deserializer) {
		auto modulus = deserializer.ReadProperty<int>(201, "modulus");
		auto remainder = deserializer.ReadProperty<int>(202, "remainder");
		return make_uniq<ModuloFilter>(modulus, remainder);
	}

	unique_ptr<TableFilterState> InitializeState(ClientContext &context) const override {
		auto result = make_uniq<ModuloFilterState>();
		result->modulus = modulus;
		return std::move(result);
	}

	idx_t Filter(Vector &vector, SelectionVector &sel, idx_t &approved_tuple_count,
	             TableFilterState &filter_state) const override {
		auto &state = filter_state.Cast<ModuloFilterState>();
		UnifiedVectorFormat vdata;
		vector.ToUnifiedFormat(approved_tuple_count, vdata);
		auto data = UnifiedVectorFormat::GetData<int64_t>(vdata);
		auto &mask = vdata.validity;

		SelectionVector new_sel(approved_tuple_count);
		idx_t result_count = 0;

		for (idx_t i = 0; i < approved_tuple_count; i++) {
			auto idx = sel.get_index(i);
			auto vector_idx = vdata.sel->get_index(idx);

			if (mask.RowIsValid(vector_idx)) {
				if (data[vector_idx] % state.modulus == 0) {
					new_sel.set_index(result_count++, idx);
				}
			}
		}

		sel.Initialize(new_sel);
		approved_tuple_count = result_count;
		return result_count;
	}
};

class ModuloOptimizerExtension : public OptimizerExtension {
public:
	ModuloOptimizerExtension() {
		optimize_function = ModuloOptimizeFunction;
	}

	static void ModuloOptimizeFunction(OptimizerExtensionInput &input, duckdb::unique_ptr<LogicalOperator> &plan) {
		if (plan->type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = plan->Cast<LogicalGet>();
			if (!get.names.empty() && get.names[0] == "i") {
				auto filter = make_uniq<ModuloFilter>(2, 0); // Keep even numbers
				get.table_filters.PushFilter(ColumnIndex(0), std::move(filter));
			}
		}

		for (auto &child : plan->children) {
			ModuloOptimizeFunction(input, child);
		}
	}
};

class ModuloTableFilterExtension : public TableFilterExtension {
public:
	string GetName() override {
		return "modulo_filter";
	}

	duckdb::unique_ptr<TableFilter> Deserialize(Deserializer &deserializer) override {
		return ModuloFilter::Deserialize(deserializer);
	}
};

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(loadable_extension_table_filter_demo, loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.table_filter_extensions.push_back(make_uniq<ModuloTableFilterExtension>());
	config.optimizer_extensions.push_back(ModuloOptimizerExtension());
}
}
