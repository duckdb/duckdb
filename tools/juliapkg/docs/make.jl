using DuckDB
using Documenter

DocMeta.setdocmeta!(DuckDB, :DocTestSetup, :(using DuckDB); recursive = true)

makedocs(;
    modules = [DuckDB],
    authors = "Kimmo Linna <kimmo.linna@gmail.com> and contributors",
    repo = "https://github.com/kimmolinna/DuckDB.jl/blob/{commit}{path}#{line}",
    sitename = "DuckDB.jl",
    format = Documenter.HTML(;
        prettyurls = get(ENV, "CI", "false") == "true",
        canonical = "https://kimmolinna.github.io/DuckDB.jl",
        assets = String[],
    ),
    pages = ["Home" => "index.md"],
)

deploydocs(; repo = "github.com/kimmolinna/DuckDB.jl", devbranch = "main")
