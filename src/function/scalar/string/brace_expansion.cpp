#include "duckdb/common/brace_expansion.hpp"

namespace duckdb {

vector<string> BraceExpansion::brace_expansion(const string &pattern){
    vector<std::string> result;
    idx_t braceOpen = pattern.find('{');
    idx_t braceClose = pattern.find('}');

    if (braceOpen == string::npos || braceClose == string::npos) {
        //FIXME throw exception
        result.push_back(pattern);
        return result;
    }

    string prefix =  pattern.substr(0, braceOpen);
    string suffix =  pattern.substr(braceClose + 1);
    string content = pattern.substr(braceOpen + 1, braceClose - braceOpen - 1);
    std::stringstream contentStream(content);

    // FIXME happy path 
    if (content.find("..") != string::npos) {
        idx_t start, end;
        char dot;
        contentStream >> start >> dot >> dot >> end;

        for (idx_t i = start; i <= end; ++i) {
            result.push_back(prefix + std::to_string(i) + suffix);
        }
    }else {
        string item;
        while (std::getline(contentStream, item, ',')) {
            result.push_back(prefix + item + suffix);
        }
    }

     return result;
}

} // namespace duckdb
