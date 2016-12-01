#!/bin/bash
java -Xmx2000m -Djava.library.path=deps/centos7_x86_64 -cp "../prototype.jar:../lib/*" com.nicta.dataint.matcher.runner.RunRfKnnSemanticTypeClassifierEvaluation $1 $2 $3 $4 $5 $6 $7 $8 $9 ${10} ${11}
