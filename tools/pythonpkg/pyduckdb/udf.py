def vectorized(func):
	"""
	Decorate a function with annotated function parameters, so DuckDB can infer that the function should be provided with pyarrow arrays and should expect pyarrow array(s) as output
	"""
	from inspect import signature
	import types
	new_func = types.FunctionType(
		func.__code__,
		func.__globals__,
		func.__name__,
		func.__defaults__,
		func.__closure__
	)
	# Construct the annotations:
	import pyarrow as pa

	new_annotations = {}
	sig = signature(func)
	sig.parameters
	for param in sig.parameters:
		new_annotations[param] = pa.lib.ChunkedArray

	new_func.__annotations__ = new_annotations
	return new_func
