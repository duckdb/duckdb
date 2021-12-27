import subprocess
from google.cloud import bigquery
from google.oauth2 import service_account

#define query
query_start_part = """
SELECT * FROM 
js(
  (SELECT -451 as x),
  x,
  "[{name:'y', type: 'float'}]",
  "function(row, emit) {
    const x = row.x;
    const memory = new WebAssembly.Memory({ initial: 256, maximum: 256 });
    const env = {
        'abortStackOverflow': _ => { throw new Error('overflow'); },
        'table': new WebAssembly.Table({ initial: 0, maximum: 0, element: 'anyfunc' }),
        'tableBase': 0,
        'memory': memory,
        'memoryBase': 1024,
        'STACKTOP': 0,
        'STACK_MAX': memory.buffer.byteLength,
    };
    const importObject = { env };"""
query_wasm_array_start = """
    var bytes = new Uint8Array(["""
query_wasm_array_end = "])"
query_end_part = """
    WebAssembly.instantiate(bytes, importObject).then(wa => {
        const exports = wa.instance.exports;
        var even = exports.even;
        emit({y: even(x)});
    });
  }
")
"""

# Get credentials
credentials = service_account.Credentials.from_service_account_file('bigquery/bigqueryudf-e3b6a563a335.json')
project_id = 'bigqueryudf'
client = bigquery.Client(credentials= credentials,project=project_id)

# Configure the query job.
job_config = bigquery.QueryJobConfig()
job_config.use_legacy_sql = True

# Compile to wasm
process = subprocess.Popen(['bigquery/bigquery_wasm_build.sh'], stdout=subprocess.PIPE, universal_newlines=True)
while True:
    output = process.stdout.readline()
    print(output.strip())
    # Do something else
    return_code = process.poll()
    if return_code is not None:
        #print('RETURN CODE', return_code)
        # Process has finished, read rest of the output 
        for output in process.stdout.readlines():
            print(output.strip())
        break

# Convert wasm file to string
wasm_array_str = ''
try:
    with open("build/test.wasm", "rb") as f:
        buf = f.read()
        for byte in buf:
            wasm_array_str += "{}, ".format(byte)
    #print('wasm file: ', wasm_array_str)
except IOError:
    print('Error while opening the wasm file')

# Make a query
QUERY = \
  query_start_part + \
  query_wasm_array_start + \
  wasm_array_str + \
  query_wasm_array_end + \
  query_end_part
#print(QUERY)
#exit()

# Perform a query.
query_job = client.query(QUERY, job_config = job_config)

# Display the result
rows = query_job.result() # Wait for the job to complete.
for row in rows:
  #print(row.f0_, ", ", row.f1_, ": ", query_job.ended - query_job.started)
  print(row.y, ": ", query_job.ended - query_job.started)