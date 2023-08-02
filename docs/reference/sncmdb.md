# Service Now CMDB Connector Reference

The [Elastic Service Now CMDB connector](../../connectors/sources/sncmdb.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

To build and run the docker image:

1. Clone this project and Create a [new connector in Kibana](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html)
2. `cd` into the project directory
3. `make docker-build`
4. `make docker-run`
5. Data should appear in Kibana

## Availability and prerequisites

A Service Now account with API access to the CMDB tables that will be ingested.

## Usage


For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).


## To update the scheduled interval to every 5 minutes

We'll need to override the configuration stored in elasticsearch.

1. Start the sync job for manually in Kibana. Wait for it to complete.
   
2. Schedule the job for every hour in Kibana.

3. Find the _id of connector configuration stored in '.elastic-connectors-v1' 
    by creating a data view in Kibana's Discover tab. 

4. Then using the _id, update the scheduling.interval field

```
curl -X POST "localhost:9200/.elastic-connectors-v1/_update/jr4uMIkBbXrbv2Y_DDc9" -H 'Content-Type: application/json' -d'
{
  "doc": {
    "scheduling": {
      "interval": "0 0/5 * * * ?"
    }
  }
}'
```
