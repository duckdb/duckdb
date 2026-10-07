// A V2 C API extension linked straight into the test binary. Statically linked extensions bind DuckDB's symbols at
// link time, so there is no vtable and get_api is never called -- but the entrypoint still receives a context, and it
// is DuckDB that has to supply one, since a static extension loads before any client connection exists.

#include "duckdb_cpp_extension.hpp"

#include <algorithm>
#include <string>
#include <vector>

namespace {

// The three data slots of a scalar function, one struct per slot: user data
// (set at registration), bind data (planted by bind), init data (planted by
// init). Exec reads all three.
struct Factor {
	int value;
};

// Bind data must be equality-comparable: the engine compares it when it compares expressions.
struct Offset {
	int value;
	bool operator==(const Offset &other) const {
		return value == other.value;
	}
};

void MaddBind(duckdb::cxx::ScalarFunction::BindInput &input) {
	const auto &factor = input.GetUserData<Factor>();
	input.SetBindData<Offset>(Offset {factor.value + 7});
}

void MaddInit(duckdb::cxx::ScalarFunction::InitInput &input) {
	input.SetInitData<int>(input.GetBindData<Offset>().value);
}

// out[i] = a[i] * factor + b[i] + offset
void MaddExec(duckdb::cxx::ScalarFunction::ExecInput &input) {
	const auto factor = input.GetUserData<Factor>().value;
	const auto offset = input.GetInitData<int>();
	const auto a = input.GetArg(0).GetView();
	const auto b = input.GetArg(1).GetView();
	auto result = input.GetResult();
	auto *out = static_cast<int32_t *>(result.GetDataMutable());
	const auto count = input.GetRowCount();
	for (duckdb::cxx::idx_t i = 0; i < count; i++) {
		out[i] = a.Data<int32_t>()[a.SelAt(i)] * factor + b.Data<int32_t>()[b.SelAt(i)] + offset;
	}
}

// ---------------------------------------------------------------------------
// cpp_demo_remote_query(path, sql, **options): the query function of the passthrough catalog type "cpp_api_demo".
// It echoes what the engine forwarded, which is what the CONNECT tests check: the attach path, the statement text
// and the attach options, rendered as "name=value;..." in name order.
// ---------------------------------------------------------------------------

struct RemoteQueryBind {
	std::string path;
	std::string sql;
	std::string options;
	bool done = false;
};

void RemoteQueryBind_(duckdb::cxx::TableFunction::BindInput &input) {
	RemoteQueryBind bind;
	bind.path = std::string(input.GetConstantArgument(0).Get<duckdb::cxx::varchar_t>().view());
	bind.sql = std::string(input.GetConstantArgument(1).Get<duckdb::cxx::varchar_t>().view());
	std::vector<std::string> options;
	for (duckdb::cxx::idx_t i = 2; i < input.GetArgCount(); i++) {
		options.push_back(input.GetArgName(i) + "=" + input.GetConstantArgument(i).ToText());
	}
	std::sort(options.begin(), options.end());
	for (auto &option : options) {
		bind.options += (bind.options.empty() ? "" : ";") + option;
	}
	// type text is parsed with the local grammar even while the client is CONNECT-ed
	auto varchar = input.GetContext().ParseType("VARCHAR");
	input.AddResultColumn("path", varchar);
	input.AddResultColumn("sql", varchar);
	input.AddResultColumn("options", varchar);
	input.SetBindData<RemoteQueryBind>(std::move(bind));
}

void RemoteQueryInitGlobal(duckdb::cxx::TableFunction::InitGlobalInput &input) {
	input.SetGlobalState<RemoteQueryBind>();
}

void RemoteQueryExec(duckdb::cxx::TableFunction::ExecInput &input) {
	const auto &bind = input.GetBindData<RemoteQueryBind>();
	auto &state = input.GetGlobalState<RemoteQueryBind>();
	auto chunk = input.GetOutputChunk();
	if (state.done) {
		chunk.GetVector(0).SetSize(0);
		return;
	}
	state.done = true;
	auto path = chunk.GetVector(0);
	auto sql = chunk.GetVector(1);
	auto options = chunk.GetVector(2);
	path.SetSize(1);
	sql.SetSize(1);
	options.SetSize(1);
	path.AssignString(0, bind.path);
	sql.AssignString(0, bind.sql);
	options.AssignString(0, bind.options);
}

} // namespace

DUCKDB_CPP_EXTENSION_ENTRYPOINT(duckdb::cxx::Extension &extension, duckdb::cxx::Context &context) {
	const auto type = context.ParseType("DECIMAL(18, 3)");
	context.Log(duckdb::cxx::LogLevel::LOG_INFO, "cpp_api_static_demo loaded, parsed " + type.ToText(),
	            "CppApiStaticDemo");

	// Register a scalar function through the C++ wrapper, exercising every data slot.
	const auto integer = context.ParseType("INTEGER");
	auto function = duckdb::cxx::ScalarFunction::Create(extension);
	function.SetName("cpp_demo_madd");
	function.GetSignature().AddParameter("a", integer).AddParameter("b", integer).SetReturnType(integer);
	function.SetUserData<Factor>(Factor {3});
	function.SetBindCallback(MaddBind);
	function.SetInitCallback(MaddInit);
	function.SetExecCallback(MaddExec);
	function.Register();

	// Register a passthrough catalog type: ATTACH 'cpp_api_demo:<path>' / CONNECT 'cpp_api_demo:<path>' route every
	// forwarded statement to cpp_demo_remote_query.
	const auto varchar = context.ParseType("VARCHAR");
	auto remote_query = duckdb::cxx::TableFunction::Create(extension);
	remote_query.SetName("cpp_demo_remote_query");
	remote_query.WithSignature([&](duckdb::cxx::FunctionSignature &sig) {
		sig.AddParameter("path", varchar);
		sig.AddParameter("sql", varchar);
		sig.AddKwargs("options", context.CreateType(duckdb::cxx::LogicalTypeId::ANY));
	});
	remote_query.SetBindCallback(RemoteQueryBind_)
	    .SetInitGlobalCallback(RemoteQueryInitGlobal)
	    .SetExecCallback(RemoteQueryExec);
	remote_query.Register();

	auto catalog_type = duckdb::cxx::RemoteCatalogType::Create(extension);
	catalog_type.SetName("cpp_api_demo").SetQueryFunction("cpp_demo_remote_query");
	catalog_type.Register();
}
