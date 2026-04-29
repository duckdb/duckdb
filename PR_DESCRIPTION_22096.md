## Title
Fix TopN cast binding rewrite

## Description
Fix a TopN window elimination rewrite bug where remapped bindings could lose cast/type alignment and trigger an internal type mismatch (`TIMESTAMPTZ != VARCHAR`).
Adds/keeps regression coverage for the `QUALIFY ROW_NUMBER() ... ORDER BY c::TIMESTAMPTZ` case from issue #22096.
