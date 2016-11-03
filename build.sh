#!/bin/bash
#
# Please check pnda-build/ for the build products

VERSION=${1}

function error {
    echo "Not Found"
    echo "Please run the build dependency installer script"
    exit -1
}

echo -n "Apache Maven 3.0.5: "
if [[ $(mvn -version 2>&1) == *"Apache Maven 3.0.5"* ]]; then
    echo "OK"
else
    error
fi

mkdir -p pnda-build
cd data-service
mvn versions:set -DnewVersion=${VERSION}
mvn clean package
cd ..
mv data-service/target/data-service-${VERSION}.tar.gz pnda-build/
sha512sum pnda-build/data-service-${VERSION}.tar.gz > pnda-build/data-service-$VERSION.tar.gz.sha512.txt

cd hdfs-cleaner
mvn versions:set -DnewVersion=${VERSION}
mvn clean package
cd ..
mv hdfs-cleaner/target/hdfs-cleaner-${VERSION}.tar.gz pnda-build/
sha512sum pnda-build/hdfs-cleaner-${VERSION}.tar.gz > pnda-build/hdfs-cleaner-${VERSION}.tar.gz.sha512.txt

