#!/bin/bash

if [[ $DB_IS_DRIVER != "TRUE" ]]; then  

  #rm /var/lib/apt/lists/*_*
  rm /var/lib/apt/lists/security*
  apt-get update

  echo "installing osrm backend server dependencies"
  sudo apt -qq install -y build-essential git cmake pkg-config libbz2-dev libxml2-dev libzip-dev libboost-all-dev lua5.2 liblua5.2-dev libtbb-dev
  
  echo "launching osrm backend server"
  /dbfs/FileStore/osrm-backend/build/osrm-routed --algorithm=MLD /dbfs/FileStore/osrm-backend/maps/north-america/us-northeast-latest.osrm &
  
  echo "wait until osrm backend server becomes responsive"
  res=-1
  i=1

  # while no response
  while [ $res -ne 0 ]
  do

    # test connectivity
    curl --silent "http://127.0.0.1:5000/route/v1/driving/-74.005310,40.708750;-73.978691,40.744850"
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
  
fi
