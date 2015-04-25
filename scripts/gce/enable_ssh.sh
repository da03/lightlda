#!/bin/bash
location="gs://ssh_keys"
private_key="$location/id_rsa"
public_key="$location/id_rsa.pub"
echo $private_key
echo $public_key

cd /home/manu/.ssh
gsutil cp $private_key ./
gsutil cp $public_key ./

cat id_rsa.pub >> authorized_keys
cat id_rsa.pub >> authorized_keys2
echo "StrictHostKeyChecking no" >> config
# rm known_hosts*
sudo chmod g-rwx id_rsa
sudo chmod g-rwx config