#include "duckdb/common/brace_expansion.hpp"

namespace duckdb {

// Not allow nested brace expansion and non-matching brace
bool BraceExpansion::has_brace_expansion(const string &pattern){
    bool hasBrace = false;
    idx_t braceCount = 0;

    for (char ch : pattern) {
        // Detects nested braces
        if (ch == '{') {
            hasBrace = true;
            if (braceCount > 0) { 
                break;
            }
            braceCount++;
        // Detects non-matching closing brace
        } else if (ch == '}') {
            if (braceCount <= 0) { 
                break;
            }
            braceCount--;
        }
    }
    // Checks if all braces are matched and there is at least one brace
    if(braceCount == 0 && hasBrace){
        return true;
    }else {
        if (braceCount > 0) {
          throw InvalidInputException("Not a vaild brace expansion file name");
        }
        // Without any brace
        return false;
    }
    
}


vector<string> BraceExpansion::brace_expansion(const string &pattern){
    vector<std::string> result;
    idx_t firstBraceOpen  = pattern.find('{');
    idx_t firstbraceClose = pattern.find('}');

    if (firstBraceOpen  == string::npos || firstbraceClose == string::npos) {
       throw InvalidInputException("Cannot find brace during brace expansion");
    }

    string prefix =  pattern.substr(0, firstBraceOpen );
    string suffix =  pattern.substr(firstbraceClose + 1);
    string content = pattern.substr(firstBraceOpen  + 1, firstbraceClose - firstBraceOpen  - 1);
    std::stringstream contentStream(content);

    // FIXME happy path 
    if(content.find("..") != string::npos){
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

    // Handle muilple brace expansion recursively
    while(!result.empty() && has_brace_expansion(result.front())){
        string pattern = result.front();
        result.erase(result.begin());
        vector<string> cartesian_product = brace_expansion(pattern);
        result.insert(result.end(), cartesian_product.begin(), cartesian_product.end());
    }

    return result;
}

} // namespace duckdb
