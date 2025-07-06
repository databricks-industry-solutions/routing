#!/bin/bash

# if [[ $DB_IS_DRIVER != "TRUE" ]]; then  

echo "installing osrm backend server dependencies"
sudo apt-get update -qq
sudo apt-get install -y --no-install-recommends \
      build-essential git cmake pkg-config \
      libboost-all-dev libtbb-dev \
      lua5.2 liblua5.2-dev \
      libbz2-dev libzip-dev \
      libxml2-dev

echo "launching osrm backend server"
source /Volumes/josh_melton/routing/osrm_backend/osrm_env.sh          # TODO: change path
rm -rf "$LOCAL_OSRM"
time cp -R "$VOLUME_BASE" "$LOCAL_OSRM"          # measure copy time

echo "Launching router ..."
cd "$LOCAL_OSRM/maps/$REGION"
"$LOCAL_OSRM/build/osrm-routed" map.osrm \
        --algorithm=MLD --max-table-size=500 \
        --port=$PORT --threads=2 >/tmp/osrm_test.log 2>&1 &

echo "wait until osrm backend server becomes responsive"
res=-1
i=1

# while no response
while [ $res -ne 0 ]
do

  # test connectivity
  curl --silent "http://127.0.0.1:5100/route/v1/driving/-73.935242,40.730610;-74.005974,40.712776"
  res=$?
  # increment the loop counter
  if [ $i -gt 40 ] 
  then 
    break
  fi
  i=$(( $i + 1 ))

  # if no response, sleep
  if [ $res -ne 0 ]
  then
    sleep 30
  fi

done  
  
# fi
