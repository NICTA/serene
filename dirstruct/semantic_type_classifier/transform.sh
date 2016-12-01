#!/bin/bash
java  -Xmx1000m -Djava.library.path=deps/centos7_x86_64 -cp "../prototype.jar:../lib/*" com.nicta.dataint.matcher.runner.RunSemanticTypeDataTransformations $1 $2 $3 $4
