import re
import sys

post_text = sys.stdin.read()

sql_keyword_list = ["select", "from", "where", "join", "group by", "order by", "having", "with recursive", "union"]
sql_keyword_regex = f"({'|'.join(sql_keyword_list)})"

sql_keywords = len(re.findall(rf"{sql_keyword_regex}", post_text, flags=re.MULTILINE | re.IGNORECASE))

backticked_code_blocks = len(re.findall(r"^```", post_text))

indented_sql_code_lines = len(re.findall(r"^{sql_keyword_regex}", post_text, flags=re.MULTILINE | re.IGNORECASE))
indented_python_code_lines = len(re.findall(r"^    (import|duckdb)", post_text, flags=re.MULTILINE | re.IGNORECASE))
indented_r_code_lines = len(re.findall(r"^    (library|dbExecute)", post_text, flags=re.MULTILINE | re.IGNORECASE))
indented_hashbang_code_lines = len(re.findall(r"^    #!", post_text, flags=re.MULTILINE | re.IGNORECASE))

indented_code_lines = indented_sql_code_lines + indented_python_code_lines + indented_r_code_lines
inline_code_snippets = len(re.findall(r"`", post_text)) // 2

print("Metrics computed by 'check-issue-for-code-formatting.py':")
print(f"- {sql_keywords} SQL keyword(s)")
print(f"- {backticked_code_blocks} backticked code block(s)")
print(
    f"- {indented_code_lines} indented code line(s): {indented_sql_code_lines} SQL, {indented_python_code_lines} Python, {indented_r_code_lines} R, {indented_hashbang_code_lines} hashbangs"
)
print(f"- {inline_code_snippets} inline code snippet(s)")

if sql_keywords > 2 and backticked_code_blocks == 0 and indented_code_lines == 0 and inline_code_snippets == 0:
    print("The post is likely not properly formatted.")
    exit(1)
else:
    print("The post is likely properly formatted.")
