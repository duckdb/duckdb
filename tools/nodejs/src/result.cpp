#include "duckdb_node.hpp"

namespace node_duckdb {

Napi::FunctionReference Result::constructor;

Napi::Object Result::Init(Napi::Env env, Napi::Object exports) {
	Napi::HandleScope scope(env);

	Napi::Function t = DefineClass(env, "Result",
	                               {
	                                   InstanceMethod("get", &Result::Get), InstanceMethod("all", &Result::All),
	                                   InstanceMethod("each", &Result::Each) //,
	                                   //    InstanceMethod("reset", &Result::Reset),
	                                   //    InstanceMethod("finalize", &Result::Finalize_)
	                               });

	constructor = Napi::Persistent(t);
	constructor.SuppressDestruct();

	exports.Set("Statement", t);
	return exports;
}

Result::Result(const Napi::CallbackInfo &info) : Napi::ObjectWrap<Result>(info) {
}

Napi::Value Result::Get(const Napi::CallbackInfo &info) {
	printf("Result::Get()\n");
}

Napi::Value Result::All(const Napi::CallbackInfo &info) {
	printf("Result::All()\n");
}

Napi::Value Result::Each(const Napi::CallbackInfo &info) {
	printf("Result::Each()\n");

	return info.This();
}

}