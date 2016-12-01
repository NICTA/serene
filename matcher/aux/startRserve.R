library("Rserve")
Rserve(args="--no-save")
system("ps -A | grep Rserve | head -n 1 | awk '{print $1}' > 'rserve_procid'")
