#include "duckdb.hpp"
#include "visualizer-extension.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/fstream.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/client_context.hpp"




namespace duckdb {



static string ToHTML(ClientContext &context)  {
	string path = VIS_RESOURCE_DIR;
    std::stringstream ss;
    ss << "<!DOCTYPE <html>\n";
    ss << "<style>\n";
    ifstream css(path + "visualizer.css");
	ss << css.rdbuf() ;
    ss << "</style>\n";
    ss << "<head>\n";
    ss << "\t<meta charset=\"utf-8\">\n";
    ss << "\t<title>Query Profile Graph for Query</title>\n";
    ss << "</head>\n";
    ss << "<body>\n";
    ss << "<script>";
    ifstream d3_js(path + "d3.js");
    ss << d3_js.rdbuf() ;
    ss << "</script>\n";
	ss << "<script> var data = ";
    ss << context.query_profiler_history->GetPrevProfilers().back().second->ToJSON();
    ss << "</script>\n";
    ss << "\n";
    ss << "<script>";
    ifstream script_js(path + "script.js");
    ss << script_js.rdbuf() ;
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

static void PragmaVisualizeProfilingOutput(ClientContext &context, const FunctionParameters &parameters) {
    // this is either enable_profiling = json, or enable_profiling = query_tree
    string file_name = parameters.values[0].ToString();
    if (file_name.empty()) {
        throw ParserException(
            "Filename not specified");
    }
	std::cout << file_name;
	WriteToFile(file_name, ToHTML(context));
}




void VisualizerExtension::Load(DuckDB &db) {
    Connection con(db);
    con.BeginTransaction();
    auto &catalog = Catalog::GetCatalog(*con.context);

    auto set_cpu_func = PragmaFunction::PragmaAssignment("visualize_profiling_output", PragmaVisualizeProfilingOutput, LogicalType::VARCHAR);
    CreatePragmaFunctionInfo info(set_cpu_func);
    catalog.CreatePragmaFunction(*con.context, &info);

    con.Commit();
}

} // namespace duckdb
