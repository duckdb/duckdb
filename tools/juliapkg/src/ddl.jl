
function drop!(db::DB, table::AbstractString; ifexists::Bool = false)
    exists = ifexists ? "IF EXISTS" : ""
    return execute(db, "DROP TABLE $exists $(esc_id(table))")
end
