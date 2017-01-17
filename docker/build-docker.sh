#!/bin/bash
#
# This script will build the uber-jar on the host machine, then
# run a docker script that contains only the jar and the executable.
# Additionally, the docker image is exported to a .tar.gz file for
# use on other machines.
#

pushd "$(dirname "$0")" > /dev/null
cd ..

#
# First build the current revision...
#
bin/build
if [ $? -eq 0 ]; then
    echo "Build completed successfully."
else
    echo "Build failed to execute"
    exit 1
fi

#
# Copy across the final jar and the executable...
#
cp jars/serene-0.1.0.jar docker/
cp bin/serene-start docker/

#
# Now we can build the docker image...
#
cd docker

echo "Building docker image..."

docker build -t serene .
if [ $? -eq 0 ]; then
    echo "Docker image constructed successfully."
else
    echo "Docker failed to build"
    exit 1
fi

#
# clean up...
#
rm serene-0.1.0.jar
rm serene-start

#
# output docker to image...
#
FILENAME=serene-$(git rev-parse HEAD | cut -c1-8).tar

echo "Exporting docker to $FILENAME..."
docker save --output $FILENAME serene
if [ $? -eq 0 ]; then
    echo "Compressing file..."
    gzip $FILENAME
    echo "Docker image exported successfully to $FILENAME.gz"
else
    echo "Docker failed to export to $FILENAME"
    exit 1
fi

#
# Notify user...
#
echo ""
echo "File exported to $FILENAME.gz. Copy onto remote machine and restore with:"
echo ""
echo " docker load --input $FILENAME.gz"
echo ""
echo "Launch with:"
echo ""
echo " docker run -d -p 9000:8080 --name serene-instance serene &"
echo ""
echo "Test with:"
echo ""
echo " curl :9000/v1.0"
echo ""
echo "Check status with:"
echo ""
echo " docker ps"
echo ""
echo "Stop server with:"
echo ""
echo " docker stop serene-instance -t 0"
echo ""
echo "Restart with:"
echo ""
echo " docker start serene-instance"
echo ""
