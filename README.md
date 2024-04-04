# udf-demo

Clone the repo, then navigate to the directory & build the jar with `mvn package`.

Mount the new jar file to your Lenses Box docker container at `plugins/`.
- Either with the `docker run` command or within the `docker-compose.yml` file

Start the Lenses Box instance, then start the mock data producer `ipaddress-mock-data-prod.py`
