#!/bin/bash

for i in $(find ./Texas/ -name "*.xml");
do
    iconv -f iso-8859-1 -t utf-8 ${i} -o ${i}.utf8
    mv ${i}.utf8 ${i}
done

echo "DONE!"
