# Databricks notebook source
# MAGIC %md 
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/routing.git. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/scalable-route-generation.

# COMMAND ----------

# MAGIC %md The purpose of this notebook is to compile the OSRM software and pre-process the OpenStreetMap files required to generate map routes from within a Databricks cluster.

# COMMAND ----------

# MAGIC %md ## Introduction
# MAGIC 
# MAGIC The steps performed in this notebook illustrate how you may prepare the assets required to run the OSRM Backend Server within a Databricks cluster.  These steps are performed infrequently, possibly even once, ahead of the launching of an OSRM-equipped Databricks cluster.  
# MAGIC 
# MAGIC To perform this work, it is recommended you run Databricks as a single-node cluster, *i.e.* a cluster with only a driver and no worker nodes. (To deploy a single node cluster, select *single node* from the *Cluster Mode* drop-down found at the top of the *Create Cluster* page.) The driver server needs to have a very large volume of RAM assigned to it relative to the size of the map files to be processed.  To give a general sense of the sizing of the driver node, we found a 128 GB RAM driver node was required to process the 11.5 GB North American *.osm.pbf* map file downloaded from the [GeoFabrik website](https://download.geofabrik.de/).
# MAGIC </p>
# MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/osrm_preprocessing_deployment2.png' width=500>

# COMMAND ----------

# MAGIC %md ## Step 1: Build the Server Software
# MAGIC 
# MAGIC To get started, we need to build the OSRM Backend Server from the current source code available in [its GitHub repository](https://github.com/Project-OSRM/osrm-backend). The [*Build from Source* instructions](https://github.com/Project-OSRM/osrm-backend#building-from-source) provided by the project team provide the basic steps required for this.  We'll start by installing package dependencies into our environment as follows:

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %sh -e
# MAGIC 
# MAGIC sudo apt -qq install -y build-essential git cmake pkg-config \
# MAGIC libbz2-dev libxml2-dev libzip-dev libboost-all-dev \
# MAGIC lua5.2 liblua5.2-dev libtbb-dev

# COMMAND ----------

# MAGIC %md We can then clone the OSRM Backend Server repository to our local system:

# COMMAND ----------

# DBTITLE 1,Clone the OSRM Backend Server Repo
# MAGIC %sh -e 
# MAGIC 
# MAGIC # make directory for repo clone
# MAGIC mkdir -p /srv/git
# MAGIC cd /srv/git
# MAGIC 
# MAGIC # clone the osrm backend server repo
# MAGIC rm -rf osrm-backend
# MAGIC git clone --depth 1 -b v5.26.0 https://github.com/Project-OSRM/osrm-backend

# COMMAND ----------

# MAGIC %md And now we build the server:
# MAGIC 
# MAGIC **NOTE** This step may take 20 minutes or more to complete depending on the size of the driver node in your Databricks cluster.

# COMMAND ----------

# DBTITLE 1,Build the OSRM Backend Server
# MAGIC %sh -e
# MAGIC 
# MAGIC cd /srv/git/osrm-backend
# MAGIC 
# MAGIC mkdir -p build
# MAGIC cd build
# MAGIC 
# MAGIC cmake ..
# MAGIC cmake --build .
# MAGIC sudo cmake --build . --target install

# COMMAND ----------

# MAGIC %md ## Step 2: Prepare the Map Files
# MAGIC 
# MAGIC The OSRM Backend Server generates routes leveraging map data. Maps for specific regions are available from the [GeoFabrik download site](https://download.geofabrik.de/) as *.osm.pbf* files.  Depending on your needs, you might leverage a map files scoped to continent, country or region levels.  
# MAGIC 
# MAGIC The file(s) you intend to use must be downloaded and pre-processed before the OSRM software can make use of it.  During pre-processing, you must choose between two pre-processing paths and select a routing profile which indicates whether routing should be based on car, foot or other means of route traversal.  More details on these options can be found [here](https://github.com/Project-OSRM/osrm-backend/wiki/Running-OSRM).  We've elected to follow the Multi-Level Dijkstra (MLD) pre-processing path as its the preferred path in the OSRM documentation using vehicle (car) traversal.
# MAGIC 
# MAGIC **NOTE** Given the size of some of the map files, the download and pre-processing steps may require quite a bit of time to complete.

# COMMAND ----------

# MAGIC %md The first step is to download the map file we intend to employ:
# MAGIC 
# MAGIC **NOTE** This step takes about 15 minutes for the North American map file.

# COMMAND ----------

# DBTITLE 1,Download Map File
# MAGIC %sh -e 
# MAGIC 
# MAGIC # create clean folder to house downloaded map file
# MAGIC rm -rf /srv/git/osrm-backend/maps/north-america
# MAGIC mkdir -p /srv/git/osrm-backend/maps/north-america
# MAGIC 
# MAGIC # download map file to appropriate folder
# MAGIC cd /srv/git/osrm-backend/maps/north-america
# MAGIC wget --quiet https://download.geofabrik.de/north-america-latest.osm.pbf
# MAGIC 
# MAGIC # list folder contents
# MAGIC ls -l .

# COMMAND ----------

# MAGIC %md Next we extract the map file assets specifying a profile indicating our selected means of route traversal. Please note, the output of the *osrm-extract* command is fairly verbose.  To avoid problems with overwhelming cell output, we've directed the standard output to a log file and then examine the last few lines of that file to verify completion:
# MAGIC 
# MAGIC **NOTE** This step takes close to 1 hour for the North American map file.

# COMMAND ----------

# DBTITLE 1,Extract Map File Content 
# MAGIC %sh -e
# MAGIC 
# MAGIC # setup location to house log files
# MAGIC mkdir -p /srv/git/osrm-backend/logs
# MAGIC 
# MAGIC # move to folder housing map file
# MAGIC cd /srv/git/osrm-backend/maps/north-america
# MAGIC 
# MAGIC # extract map file contents
# MAGIC /srv/git/osrm-backend/build/osrm-extract north-america-latest.osm.pbf -p /srv/git/osrm-backend/profiles/car.lua > /srv/git/osrm-backend/logs/extract_log.txt
# MAGIC 
# MAGIC # review output from extract command
# MAGIC #echo '----------------------------------------'
# MAGIC #tail /srv/git/osrm-backend/logs/extract_log.txt

# COMMAND ----------

# MAGIC %md Before proceeding, be sure to check the last few lines of output from the *osrm-extract* command.  If you did not receive a message towards the end for ***\[info\] RAM: peak bytes used*** then it is likely the extract process crashed due to insufficient memory on the driver node of your cluster.  In the event of such a crash, you will not necessarily receive an error in the command output.
# MAGIC 
# MAGIC Even if you received the expected message regarding RAM usage, it's a good idea to verify the extract completed successfully by confirming you have a large number of files now present in the folder where the *.osm.pbf* file resides:

# COMMAND ----------

# DBTITLE 1,Verify Map File Extraction
# MAGIC %sh -e ls -l /srv/git/osrm-backend/maps/north-america

# COMMAND ----------

# MAGIC %md The next step in the MLD pre-processing path is to partition the content from the extracted files:
# MAGIC 
# MAGIC **NOTE** This step takes close to 1 hour for the North American map file.

# COMMAND ----------

# DBTITLE 1,Partition Extracted Map Files
# MAGIC %sh -e 
# MAGIC 
# MAGIC cd /srv/git/osrm-backend/maps/north-america
# MAGIC 
# MAGIC /srv/git/osrm-backend/build/osrm-partition north-america-latest.osrm

# COMMAND ----------

# MAGIC %md And lastly, we customize the content per the instructions associated with this pre-processing path:

# COMMAND ----------

# DBTITLE 1,Customize Extracted Map Files
# MAGIC %sh -e 
# MAGIC 
# MAGIC cd /srv/git/osrm-backend/maps/north-america
# MAGIC 
# MAGIC /srv/git/osrm-backend/build/osrm-customize north-america-latest.osrm

# COMMAND ----------

# MAGIC %md ## Step 3: Persist OSRM Assets
# MAGIC 
# MAGIC The OSRM Backend Server and associated map assets have been created in the */srv/git* folder on our cluster's driver node.  This folder is transient and only accessible to the driver node. This means that when this cluster is stopped (terminated) all these assets will be lost.  To persist these assets for re-use between cluster restarts, we need to copy these to a persistent location. Within Databricks, we might chose to make use of a [cloud storage mount](https://docs.databricks.com/data/databricks-file-system.html#mount-object-storage-to-dbfs).  We might also simply make use of the build-in [FileStore](https://docs.databricks.com/data/filestore.html) storage location to keep things simple:

# COMMAND ----------

# DBTITLE 1,Copy OSRM Assets to Persistent Location
# MAGIC %sh -e
# MAGIC 
# MAGIC rm -rf /dbfs/FileStore/osrm-backend
# MAGIC cp -L -R /srv/git/osrm-backend /dbfs/FileStore/osrm-backend

# COMMAND ----------

# MAGIC %md ## Step 4: Create Init Script
# MAGIC 
# MAGIC At this point, all the elements required to run the OSRM Backend Server are in place.  We now need to define a [cluster init script](https://docs.databricks.com/clusters/init-scripts.html#cluster-scoped-init-scripts) with which the server can be deployed to each worker node in a Databricks cluster.
# MAGIC 
# MAGIC The logic within this script is pretty straightforward.  We install the package dependencies (pretty much as we did above) and the launch the routing server.  
# MAGIC 
# MAGIC The routing server can take a while to become responsive to routing requests. We've added logic to test the server and wait until it is fully responsive.  This can delay the cluster using this script from being fully accessible to run jobs for a few minutes but better ensures that logic run immediately after cluster start, such as logic executed as part of with a workflow (job), will succeed.
# MAGIC 
# MAGIC Please note the init script is being written to an accessible location within the DBFS file system. Such a location is required for a cluster to access the script during startup:

# COMMAND ----------

# DBTITLE 1,Create Init Script
# make folder to house init script
dbutils.fs.mkdirs('dbfs:/databricks/scripts')

# write init script
dbutils.fs.put(
  '/databricks/scripts/osrm-backend.sh',
  '''
#!/bin/bash

if [[ $DB_IS_DRIVER != "TRUE" ]]; then  

  echo "installing osrm backend server dependencies"
  sudo apt -qq install -y build-essential git cmake pkg-config libbz2-dev libxml2-dev libzip-dev libboost-all-dev lua5.2 liblua5.2-dev libtbb-dev
  
  echo "launching osrm backend server"
  /dbfs/FileStore/osrm-backend/build/osrm-routed --algorithm=MLD /dbfs/FileStore/osrm-backend/maps/north-america/north-america-latest.osrm &
  
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
''', 
  True
  )

# show script content
print(
  dbutils.fs.head('dbfs:/databricks/scripts/osrm-backend.sh')
  )

# COMMAND ----------

# MAGIC %md With the init script defined, we can now configure a cluster on which we will run the OSRM Backend Server.  Please refer to guidance in notebook *RT 00* about the sizing of the worker nodes in this cluster.
# MAGIC 
# MAGIC Note that the init script setup steps are automated for you if you use the RUNME file available in this folder to create the job and clusters for this accelerator. If you define the cluster's configuration manually, provide the path to the init script written by the previous cell.  As the cluster starts, each node in the cluster will execute this script to affect its configuration:
# MAGIC </p>
# MAGIC 
# MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/osrm_cluster_config.PNG' width=600>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC | library                                | description             | license    | source                                              |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|
# MAGIC | OSRM Backend Server                                  | High performance routing engine written in C++14 designed to run on OpenStreetMap data | BSD 2-Clause "Simplified" License    | https://github.com/Project-OSRM/osrm-backend                   |
# MAGIC | Mosaic | An extension to the Apache Spark framework that allows easy and fast processing of very large geospatial datasets | Databricks License| https://github.com/databrickslabs/mosaic | 
# MAGIC | Tabulate | pretty-print tabular data in Python | MIT License | https://pypi.org/project/tabulate/ |
