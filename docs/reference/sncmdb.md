# Service Now CMDB Connector Reference

The [Elastic Service Now CMDB connector](../../connectors/sources/sncmdb.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

To build and run the docker image:

1. Clone this project and Create a [new connector in Kibana](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html)
2. `cd` into the project directory
3. [Create a Python virtual environment](https://docs.python.org/3/library/venv.html)
4. Edit config.yml and add connector id and es details
5. `make dev`
6. `make docker-build`
7. `make docker-run` 
8. Data should appear in Kibana

## Availability and prerequisites

A Service Now account with API access to the CMDB tables that will be ingested.

## Usage


For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).


