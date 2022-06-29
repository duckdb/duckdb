/^static inline To bitwise_cast(From from) {/
{if (prev == "template <typename To, typename From>") {f=1; print "namespace duckdb_apache { namespace thrift {"}}
NR>1{print prev}
{if (prev == "}" && f) {print "}} // namespace duckdb_apache::thrift"; f=0}}
{prev=$0}
END{print prev}