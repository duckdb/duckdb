#include "duckdb.hpp"
#include "visualizer-extension.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/fstream.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

static string ToHTML(ClientContext &context, const string &first_json_path,  const string &second_json_path) {
	string path = VIS_RESOURCE_DIR;
	std::stringstream ss;
	ss << "<!DOCTYPE <html>\n";
	ss << "<style>\n";
	ifstream css(path + "visualizer.css");
	ss << css.rdbuf();
	ss << "</style>\n";
	ss << "<head>\n";
	ss << "\t<meta charset=\"utf-8\">\n";
	ss << "\t<title>Query Profile Graph for Query</title>\n";
	ss << "</head>\n";
	ss << "<body>\n";
	ss << "<script>";
	ifstream d3_js(path + "d3.js");
	ss << d3_js.rdbuf();
	ss << "</script>\n";
	ss << "<script> var data = ";
	if(first_json_path.empty()) {
		auto &lastProfiler = context.query_profiler_history->GetPrevProfilers().back().second;
		ss << lastProfiler->ToJSON();
	}
	else {
        ifstream data_json(first_json_path);
		ss << data_json.rdbuf();
	}
	ss << "</script>\n";
    ss << "<script> var secondData = ";
    if(second_json_path.empty()) {
        ss << "null;";
    }
    else {
        ifstream data_json(second_json_path);
        ss << data_json.rdbuf();
    }
    ss << "</script>\n";
	ss << "\n";
	ss << "<script>";
	ifstream script_js(path + "script.js");
	ss << script_js.rdbuf();
	ss << "</script>\n";
	ss << "</body>\n";
	ss << "</html>\n";
	return ss.str();
}

static void WriteToFile(string &path, string info) {
	ofstream out(path);
	out << info;
	out.close();
}

static void PragmaVisualizeLastProfilingOutput(ClientContext &context, const FunctionParameters &parameters) {
	// this is either enable_profiling = json, or enable_profiling = query_tree
	string file_name = parameters.values[0].ToString();
	if (file_name.empty()) {
		throw ParserException("Filename not specified");
	}
	WriteToFile(file_name, ToHTML(context, "", ""));
}

static void PragmaVisualizeJsonProfilingOutput(ClientContext &context, const FunctionParameters &parameters) {
    // this is either enable_profiling = json, or enable_profiling = query_tree
    string file_name = parameters.values[0].ToString();
    if (file_name.empty()) {
        throw ParserException("Filename not specified");
    }
    string json_path = parameters.values[1].ToString();
    if (file_name.empty()) {
        throw ParserException("JsonPath not specified");
    }
    WriteToFile(file_name, ToHTML(context, json_path, ""));
}

static void PragmaVisualizeDiffProfilingOutput(ClientContext &context, const FunctionParameters &parameters) {
    // this is either enable_profiling = json, or enable_profiling = query_tree
    string file_name = parameters.values[0].ToString();
    if (file_name.empty()) {
        throw ParserException("Filename not specified");
    }
    string first_json_path = parameters.values[1].ToString();
    if (file_name.empty()) {
        throw ParserException("First JsonPath not specified");
    }
    string second_json_path = parameters.values[2].ToString();
    if (file_name.empty()) {
        throw ParserException("Second JsonPath not specified");
    }
    WriteToFile(file_name, ToHTML(context, first_json_path, second_json_path));
}

void VisualizerExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();
	auto &catalog = Catalog::GetCatalog(*con.context);

	auto vis_last_profiler_out_func = PragmaFunction::PragmaAssignment("visualize_last_profiling_output", PragmaVisualizeLastProfilingOutput,
	                                                     LogicalType::VARCHAR);
	CreatePragmaFunctionInfo vis_last_profiler_out_info(vis_last_profiler_out_func);
	catalog.CreatePragmaFunction(*con.context, &vis_last_profiler_out_info);

    auto vis_json_func = PragmaFunction::PragmaCall("visualize_json_profiling_output", PragmaVisualizeJsonProfilingOutput,
                                                          {LogicalType::VARCHAR, LogicalType::VARCHAR});
    CreatePragmaFunctionInfo vis_json_func_info(vis_json_func);
    catalog.CreatePragmaFunction(*con.context, &vis_json_func_info);

    auto vis_json_diff_func = PragmaFunction::PragmaCall("visualize_diff_profiling_output", PragmaVisualizeDiffProfilingOutput,
                                                    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR});
    CreatePragmaFunctionInfo vis_json_diff_func_info(vis_json_diff_func);
    catalog.CreatePragmaFunction(*con.context, &vis_json_diff_func_info);



	con.Commit();
}

} // namespace duckdb
