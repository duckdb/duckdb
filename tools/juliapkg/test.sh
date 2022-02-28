set -e

julia -e "import Pkg; Pkg.activate(\".\"); include(\"test/runtests.jl\")" $1
