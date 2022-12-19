#/bin/bash
# gcloud compute regions list
gcloud config set compute/region us-central1
# gcloud computer zones list
gcloud config set compute/zone us-central1-c

for s in m1 w{1..3}
do
gcloud compute instances create $s
done
gcloud compute instances list
gcloud compute ssh $s uname -a

