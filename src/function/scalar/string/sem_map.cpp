#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include <curl/curl.h>
#include <nlohmann/json.hpp>


namespace duckdb {

std::string api_key = "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";

// 回调函数用于接收HTTP响应数据
size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* userp) {
    userp->append((char*)contents, size * nmemb);
    return size * nmemb;
}

std::string ask_LLM(const std::string& prompt) {
    CURL* curl = curl_easy_init();
    std::string response;
    
    if(curl) {
        // 准备JSON请求体
        nlohmann::json request_body = {
            {"model", "google/gemma-3-27b-it:free"},
            {"messages", {{
                {"role", "user"},
                {"content", prompt}
            }}}
        };
        
        std::string json_str = request_body.dump();
        
        // 设置请求头和URL
        struct curl_slist* headers = NULL;
        headers = curl_slist_append(headers, ("Authorization: Bearer " + api_key).c_str());
        headers = curl_slist_append(headers, "Content-Type: application/json");
        
        curl_easy_setopt(curl, CURLOPT_URL, "https://openrouter.ai/api/v1/chat/completions");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_str.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
        
        // 发送请求 - 最多尝试3次
        CURLcode res = CURLE_OK;
        int max_attempts = 3;
        int attempt = 0;
        
        while (attempt < max_attempts) {
            res = curl_easy_perform(curl);
            
            if (res == CURLE_OK) {
                // 请求成功，跳出循环
                break;
            } else {
                // 请求失败，尝试下一次
                attempt++;
                if (attempt >= max_attempts) {
                    response = "Error: " + std::string(curl_easy_strerror(res));
                }
            }
        }
        
        // 清理
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
    }
    
    // 解析响应JSON并提取回复内容
    try {
        auto json_response = nlohmann::json::parse(response);
        std::string content = json_response["choices"][0]["message"]["content"];
        
        // 去除字符串两端的空白字符（类似Python的strip函数）
        auto start = content.find_first_not_of(" \t\n\r\f\v");
        if (start == std::string::npos) {
            content = ""; // 字符串只包含空白字符
        } else {
            auto end = content.find_last_not_of(" \t\n\r\f\v");
            content = content.substr(start, end - start + 1);
        }
        
        return content;
    } catch(const std::exception& e) {
        return "Error parsing response: " + std::string(e.what());
    }
}

// 实现sem_map函数的操作
struct SemMapOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto input_data = input.GetData();
		auto input_length = input.GetSize();
		
		std::string mapped_str = ask_LLM(std::string(input_data, input_length));
		
		auto result_str = StringVector::AddString(result, mapped_str);
		return result_str;
	}
};

// 注册sem_map函数
static void SemMapFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::ExecuteString<string_t, string_t, SemMapOperator>(args.data[0], result, args.size());
}

// 获取函数定义
ScalarFunction SemMapFun::GetFunction() {
	return ScalarFunction("sem_map", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SemMapFunction);
}

} // namespace duckdb 