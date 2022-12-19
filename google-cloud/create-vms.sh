#/bin/bash
set -x 
# gcloud compute regions list
gcloud config set compute/region ${REGION}
# gcloud computer zones list
gcloud config set compute/zone ${ZONE}

for s in m1 w{1..3}
do
gcloud compute instances create $s
gcloud compute ssh $s --command 'uname -a'
gcloud compute scp install-docker.sh ${s}:/tmp
gcloud compute ssh $s --command 'bash -x /tmp/install-docker.sh'
done
gcloud compute instances list

