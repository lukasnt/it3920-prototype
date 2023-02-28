## Development Setup
#### Requirements
- [Docker](https://docs.docker.com/get-docker/) installed


Run docker compose setup with the following commands:

```
docker compose up

```


Note that building the docker image for the first time takes quite some time (around 5 min) because of needing to install all the binaries and distributions (Spark, Zeppelin, JDKs, Maven packages etc.). Subsequent runs should take as long time (only a few seconds) as it only sets up the containers specified in compose.yaml.

To access the Zeppelin notebooks open up http://0.0.0.0:8080/#/ in your browser. See the [Explore Apache Zeppelin UI](https://zeppelin.apache.org/docs/0.10.0/quickstart/explore_ui.html) for more info about navigating in the notebooks.

The main notebook should be located at http://0.0.0.0:8080/#/notebook/2HSV6W3AT which automatically runs at the compose command and starts the spark interpreter. Rest of the visualizations and integration tests with spark are located under the Tests folder.
