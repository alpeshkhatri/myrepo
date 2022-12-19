#!/bin/bash
ZONE=
PROJECT=
for s in m1.${ZONE}.${PROJECT} w{1..3}.${ZONE}.${PROJECT}
do
scp install-docker.sh ${s}:/tmp/
ssh $s bash /tmp/install-docker.sh
done
