#include "duckdb/common/http_util.hpp"

namespace duckdb {

HTTPStatusCode HTTPUtil::ToStatusCode(int32_t status_code) {
	switch (status_code) {
	case 100:
		return HTTPStatusCode::Continue_100;
	case 101:
		return HTTPStatusCode::SwitchingProtocol_101;
	case 102:
		return HTTPStatusCode::Processing_102;
	case 103:
		return HTTPStatusCode::EarlyHints_103;
	case 200:
		return HTTPStatusCode::OK_200;
	case 201:
		return HTTPStatusCode::Created_201;
	case 202:
		return HTTPStatusCode::Accepted_202;
	case 203:
		return HTTPStatusCode::NonAuthoritativeInformation_203;
	case 204:
		return HTTPStatusCode::NoContent_204;
	case 205:
		return HTTPStatusCode::ResetContent_205;
	case 206:
		return HTTPStatusCode::PartialContent_206;
	case 207:
		return HTTPStatusCode::MultiStatus_207;
	case 208:
		return HTTPStatusCode::AlreadyReported_208;
	case 226:
		return HTTPStatusCode::IMUsed_226;
	case 300:
		return HTTPStatusCode::MultipleChoices_300;
	case 301:
		return HTTPStatusCode::MovedPermanently_301;
	case 302:
		return HTTPStatusCode::Found_302;
	case 303:
		return HTTPStatusCode::SeeOther_303;
	case 304:
		return HTTPStatusCode::NotModified_304;
	case 305:
		return HTTPStatusCode::UseProxy_305;
	case 306:
		return HTTPStatusCode::unused_306;
	case 307:
		return HTTPStatusCode::TemporaryRedirect_307;
	case 308:
		return HTTPStatusCode::PermanentRedirect_308;
	case 400:
		return HTTPStatusCode::BadRequest_400;
	case 401:
		return HTTPStatusCode::Unauthorized_401;
	case 402:
		return HTTPStatusCode::PaymentRequired_402;
	case 403:
		return HTTPStatusCode::Forbidden_403;
	case 404:
		return HTTPStatusCode::NotFound_404;
	case 405:
		return HTTPStatusCode::MethodNotAllowed_405;
	case 406:
		return HTTPStatusCode::NotAcceptable_406;
	case 407:
		return HTTPStatusCode::ProxyAuthenticationRequired_407;
	case 408:
		return HTTPStatusCode::RequestTimeout_408;
	case 409:
		return HTTPStatusCode::Conflict_409;
	case 410:
		return HTTPStatusCode::Gone_410;
	case 411:
		return HTTPStatusCode::LengthRequired_411;
	case 412:
		return HTTPStatusCode::PreconditionFailed_412;
	case 413:
		return HTTPStatusCode::PayloadTooLarge_413;
	case 414:
		return HTTPStatusCode::UriTooLong_414;
	case 415:
		return HTTPStatusCode::UnsupportedMediaType_415;
	case 416:
		return HTTPStatusCode::RangeNotSatisfiable_416;
	case 417:
		return HTTPStatusCode::ExpectationFailed_417;
	case 418:
		return HTTPStatusCode::ImATeapot_418;
	case 421:
		return HTTPStatusCode::MisdirectedRequest_421;
	case 422:
		return HTTPStatusCode::UnprocessableContent_422;
	case 423:
		return HTTPStatusCode::Locked_423;
	case 424:
		return HTTPStatusCode::FailedDependency_424;
	case 425:
		return HTTPStatusCode::TooEarly_425;
	case 426:
		return HTTPStatusCode::UpgradeRequired_426;
	case 428:
		return HTTPStatusCode::PreconditionRequired_428;
	case 429:
		return HTTPStatusCode::TooManyRequests_429;
	case 431:
		return HTTPStatusCode::RequestHeaderFieldsTooLarge_431;
	case 451:
		return HTTPStatusCode::UnavailableForLegalReasons_451;
	case 500:
		return HTTPStatusCode::InternalServerError_500;
	case 501:
		return HTTPStatusCode::NotImplemented_501;
	case 502:
		return HTTPStatusCode::BadGateway_502;
	case 503:
		return HTTPStatusCode::ServiceUnavailable_503;
	case 504:
		return HTTPStatusCode::GatewayTimeout_504;
	case 505:
		return HTTPStatusCode::HttpVersionNotSupported_505;
	case 506:
		return HTTPStatusCode::VariantAlsoNegotiates_506;
	case 507:
		return HTTPStatusCode::InsufficientStorage_507;
	case 508:
		return HTTPStatusCode::LoopDetected_508;
	case 510:
		return HTTPStatusCode::NotExtended_510;
	case 511:
		return HTTPStatusCode::NetworkAuthenticationRequired_511;
	default:
		return HTTPStatusCode::INVALID;
	}
}

string HTTPUtil::GetStatusMessage(HTTPStatusCode status) {
	switch (status) {
	case HTTPStatusCode::Continue_100:
		return "Continue";
	case HTTPStatusCode::SwitchingProtocol_101:
		return "Switching Protocol";
	case HTTPStatusCode::Processing_102:
		return "Processing";
	case HTTPStatusCode::EarlyHints_103:
		return "Early Hints";
	case HTTPStatusCode::OK_200:
		return "OK";
	case HTTPStatusCode::Created_201:
		return "Created";
	case HTTPStatusCode::Accepted_202:
		return "Accepted";
	case HTTPStatusCode::NonAuthoritativeInformation_203:
		return "Non-Authoritative Information";
	case HTTPStatusCode::NoContent_204:
		return "No Content";
	case HTTPStatusCode::ResetContent_205:
		return "Reset Content";
	case HTTPStatusCode::PartialContent_206:
		return "Partial Content";
	case HTTPStatusCode::MultiStatus_207:
		return "Multi-Status";
	case HTTPStatusCode::AlreadyReported_208:
		return "Already Reported";
	case HTTPStatusCode::IMUsed_226:
		return "IM Used";
	case HTTPStatusCode::MultipleChoices_300:
		return "Multiple Choices";
	case HTTPStatusCode::MovedPermanently_301:
		return "Moved Permanently";
	case HTTPStatusCode::Found_302:
		return "Found";
	case HTTPStatusCode::SeeOther_303:
		return "See Other";
	case HTTPStatusCode::NotModified_304:
		return "Not Modified";
	case HTTPStatusCode::UseProxy_305:
		return "Use Proxy";
	case HTTPStatusCode::unused_306:
		return "unused";
	case HTTPStatusCode::TemporaryRedirect_307:
		return "Temporary Redirect";
	case HTTPStatusCode::PermanentRedirect_308:
		return "Permanent Redirect";
	case HTTPStatusCode::BadRequest_400:
		return "Bad Request";
	case HTTPStatusCode::Unauthorized_401:
		return "Unauthorized";
	case HTTPStatusCode::PaymentRequired_402:
		return "Payment Required";
	case HTTPStatusCode::Forbidden_403:
		return "Forbidden";
	case HTTPStatusCode::NotFound_404:
		return "Not Found";
	case HTTPStatusCode::MethodNotAllowed_405:
		return "Method Not Allowed";
	case HTTPStatusCode::NotAcceptable_406:
		return "Not Acceptable";
	case HTTPStatusCode::ProxyAuthenticationRequired_407:
		return "Proxy Authentication Required";
	case HTTPStatusCode::RequestTimeout_408:
		return "Request Timeout";
	case HTTPStatusCode::Conflict_409:
		return "Conflict";
	case HTTPStatusCode::Gone_410:
		return "Gone";
	case HTTPStatusCode::LengthRequired_411:
		return "Length Required";
	case HTTPStatusCode::PreconditionFailed_412:
		return "Precondition Failed";
	case HTTPStatusCode::PayloadTooLarge_413:
		return "Payload Too Large";
	case HTTPStatusCode::UriTooLong_414:
		return "URI Too Long";
	case HTTPStatusCode::UnsupportedMediaType_415:
		return "Unsupported Media Type";
	case HTTPStatusCode::RangeNotSatisfiable_416:
		return "Range Not Satisfiable";
	case HTTPStatusCode::ExpectationFailed_417:
		return "Expectation Failed";
	case HTTPStatusCode::ImATeapot_418:
		return "I'm a teapot";
	case HTTPStatusCode::MisdirectedRequest_421:
		return "Misdirected Request";
	case HTTPStatusCode::UnprocessableContent_422:
		return "Unprocessable Content";
	case HTTPStatusCode::Locked_423:
		return "Locked";
	case HTTPStatusCode::FailedDependency_424:
		return "Failed Dependency";
	case HTTPStatusCode::TooEarly_425:
		return "Too Early";
	case HTTPStatusCode::UpgradeRequired_426:
		return "Upgrade Required";
	case HTTPStatusCode::PreconditionRequired_428:
		return "Precondition Required";
	case HTTPStatusCode::TooManyRequests_429:
		return "Too Many Requests";
	case HTTPStatusCode::RequestHeaderFieldsTooLarge_431:
		return "Request Header Fields Too Large";
	case HTTPStatusCode::UnavailableForLegalReasons_451:
		return "Unavailable For Legal Reasons";
	case HTTPStatusCode::NotImplemented_501:
		return "Not Implemented";
	case HTTPStatusCode::BadGateway_502:
		return "Bad Gateway";
	case HTTPStatusCode::ServiceUnavailable_503:
		return "Service Unavailable";
	case HTTPStatusCode::GatewayTimeout_504:
		return "Gateway Timeout";
	case HTTPStatusCode::HttpVersionNotSupported_505:
		return "HTTP Version Not Supported";
	case HTTPStatusCode::VariantAlsoNegotiates_506:
		return "Variant Also Negotiates";
	case HTTPStatusCode::InsufficientStorage_507:
		return "Insufficient Storage";
	case HTTPStatusCode::LoopDetected_508:
		return "Loop Detected";
	case HTTPStatusCode::NotExtended_510:
		return "Not Extended";
	case HTTPStatusCode::NetworkAuthenticationRequired_511:
		return "Network Authentication Required";

	default:
	case HTTPStatusCode::InternalServerError_500:
		return "Internal Server Error";
	}
}

} // namespace duckdb
