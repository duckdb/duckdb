set -e

julia -e "import Pkg; Pkg.activate(\".\"); Pkg.instantiate(); include(\"test/runtests.jl\")" $1
