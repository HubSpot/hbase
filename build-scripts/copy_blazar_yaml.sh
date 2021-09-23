#!/bin/bash

poms=$(find . -name pom.xml)

for pom in $poms; do
    module=$(dirname $pom)
    if [[ "$module" != "." && "$module" != "./hbase-assembly" ]]; then
        cp .blazar.yaml $module/.blazar.yaml
    fi
done
