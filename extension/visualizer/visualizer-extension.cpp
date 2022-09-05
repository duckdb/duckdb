#define DUCKDB_EXTENSION_MAIN
#include "duckdb.hpp"
#include "visualizer-extension.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/fstream.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "visualizer_constants.hpp"

namespace duckdb {

static string ToHTML(ClientContext &context, const string &first_json_path, const string &second_json_path) {
	std::stringstream ss;
	ss << "<!DOCTYPE <html>\n";
	ss << "<style>\n";
	ss << css;
	ss << "</style>\n";
	ss << "<head>\n";
	ss << "\t<meta charset=\"utf-8\">\n";
	ss << "\t<title>Query Profile Graph for Query</title>\n";
	ss << "</head>\n";
	ss << "<body>\n";
	ss << "<script>";
	ss << d3;
	ss << "</script>\n";
	ss << "<script> var data = ";
	// If no json_file is given and profiler history is not empty, read from query profiler. Else read from file.
	auto &prevProfilers = ClientData::Get(context).query_profiler_history->GetPrevProfilers();
	if (first_json_path.empty() && !prevProfilers.empty()) {
		ss << prevProfilers.back().second->ToJSON();
	} else if (!first_json_path.empty()) {
		ifstream data_json(first_json_path);
		ss << data_json.rdbuf();
		// throw an IO exception if it fails to read the json_file
		if (data_json.fail()) {
			throw IOException(strerror(errno));
		}
	}
	ss << "</script>\n";
	ss << "<script> var secondData = ";
	// If no second json_file is given, make second json_data null. Else read from file.
	if (second_json_path.empty()) {
		ss << "null;";
	} else if (!second_json_path.empty()) {
		ifstream data_json(second_json_path);
		ss << data_json.rdbuf();
		// throw an IO exception if it fails to read the json_file
		if (data_json.fail()) {
			throw IOException(strerror(errno));
		}
	}
	ss << "</script>\n";
	ss << "\n";
	ss << "<script>";
	ss << script;
	ss << "</script>\n";
	ss << "</body>\n";
	ss << "</html>\n";
	return ss.str();
}

static void WriteToFile(string &path, string info) {
	ofstream out(path);
	out << info;
	out.close();
	// throw an IO exception if it fails to write to the file
	if (out.fail()) {
		throw IOException(strerror(errno));
	}
}

static void PragmaVisualizeLastProfilingOutput(ClientContext &context, const FunctionParameters &parameters) {
	string file_name = parameters.values[0].ToString();
	if (file_name.empty()) {
		Printer::Print(ToHTML(context, "", ""));
		return;
	}
	WriteToFile(file_name, ToHTML(context, "", ""));
}

static void PragmaVisualizeJsonProfilingOutput(ClientContext &context, const FunctionParameters &parameters) {
	string file_name = parameters.values[0].ToString();
	string json_path = parameters.values[1].ToString();
	if (json_path.empty()) {
		throw ParserException("JsonPath not specified");
	}
	if (file_name.empty()) {
		Printer::Print(ToHTML(context, json_path, ""));
		return;
	}
	WriteToFile(file_name, ToHTML(context, json_path, ""));
}

static void PragmaVisualizeDiffProfilingOutput(ClientContext &context, const FunctionParameters &parameters) {
	string file_name = parameters.values[0].ToString();
	string first_json_path = parameters.values[1].ToString();
	string second_json_path = parameters.values[2].ToString();
	if (first_json_path.empty()) {
		throw ParserException("First JsonPath not specified");
	}
	if (second_json_path.empty()) {
		throw ParserException("Second JsonPath not specified");
	}
	if (file_name.empty()) {
		Printer::Print(ToHTML(context, first_json_path, second_json_path));
		return;
	}

	WriteToFile(file_name, ToHTML(context, first_json_path, second_json_path));
}

void VisualizerExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();
	auto &catalog = Catalog::GetCatalog(*con.context);

	auto vis_last_profiler_out_func = PragmaFunction::PragmaCall(
	    "visualize_last_profiling_output", PragmaVisualizeLastProfilingOutput, {LogicalType::VARCHAR});
	CreatePragmaFunctionInfo vis_last_profiler_out_info(vis_last_profiler_out_func);
	catalog.CreatePragmaFunction(*con.context, &vis_last_profiler_out_info);

	auto vis_json_func =
	    PragmaFunction::PragmaCall("visualize_json_profiling_output", PragmaVisualizeJsonProfilingOutput,
	                               {LogicalType::VARCHAR, LogicalType::VARCHAR});
	CreatePragmaFunctionInfo vis_json_func_info(vis_json_func);
	catalog.CreatePragmaFunction(*con.context, &vis_json_func_info);

	auto vis_json_diff_func =
	    PragmaFunction::PragmaCall("visualize_diff_profiling_output", PragmaVisualizeDiffProfilingOutput,
	                               {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR});
	CreatePragmaFunctionInfo vis_json_diff_func_info(vis_json_diff_func);
	catalog.CreatePragmaFunction(*con.context, &vis_json_diff_func_info);

	con.Commit();
}

std::string VisualizerExtension::Name() {
	return "visualizer";
}
} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void visualizer_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::VisualizerExtension>();
}

DUCKDB_EXTENSION_API const char *visualizer_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
