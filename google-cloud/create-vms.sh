#/bin/bash
ZONE=
PROJECT=
REGION=
# gcloud compute regions list
gcloud config set compute/region ${REGION}
# gcloud computer zones list
gcloud config set compute/zone ${ZONE}

for s in m1 w{1..3}
do
gcloud compute instances create $s
done
gcloud compute instances list
gcloud compute ssh $s uname -a

