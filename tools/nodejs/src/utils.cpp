namespace node_duckdb {

bool Utils::OtherIsInt(Napi::Number source) {
	double orig_val = source.DoubleValue();
	double int_val = (double)source.Int32Value();
	if (orig_val == int_val) {
		return true;
	} else {
		return false;
	}
}

Napi::Value Utils::CreateError(Napi::Env env, std::string msg) {
    return Napi::Error::New(env, Napi::String::New(env, msg).Utf8Value()).Value();
}



}