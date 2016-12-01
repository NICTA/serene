#!/bin/bash
java -Xmx2000m -Djava.library.path=deps/centos7_x86_64 -cp "../prototype.jar:../lib/*" com.nicta.dataint.matcher.runner.RunRfKnnSemanticTypeClassifier $1 $2 $3
