# streaming-data-pipeline
Streaming pipeline repo for data engineering training program.

See producers and consumers set up README in their respective directories.

# local environment setup
Make sure you have sbt installed.
Make sure you have docker installed.

Run `./sbin/buildAndRunLocal.sh` to create Docker containers for running and testing this setup on your local machine

To have more control and reduce memory consumption, here's how you run individual parts and troubleshoot them

* Run the base infrastructure:
  ```
  $ docker/docker-compose.sh up --build -d kafka hadoop hadoop-seed zookeeper zookeeper-seed
  ```
* Run individual services, e.g 
  ```
  $ docker/docker-compose.sh up --build -d station-san-francisco-producer station-consumer
  ```
* Tail all logs or only for a specific service
  ```
  $ docker/docker-compose.sh logs -f
  $ docker/docker-compose.sh logs -f station-consumer
  ```
