# Application Graph in Neo4j
This repo contains code to process data about applications and populate a Neo4j graph.

## Installation
This step assumes that Python 3.8+ is installed. Set up a virtual environment and install from requirements.txt:

```sh
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip3 install -r requirements.txt
```

For further development, simply activate the existing virtual environment.

```sh
$ source .venv/bin/activate
```

## Preprocessing

The provided data contains IDs to applications that are stored as strings -- for more efficiently populating the graph, these need to be converted to long integers, which we do using Pandas. To clean the string IDs and other miscellaneous data cleaning tasks for building the graph, run the preprocessing script (after editing the path to the raw data appropriately) as follows:

```sh
$ python3 clean_data.py
```

This script takes in raw CSV files from the `graph_data` directory and populates clean, formatted CSVs into the `graph_data_clean` directory.

## Building the graph
The ideal way to build a large graph from CSV is using the [`neo4j-admin` import tool](https://neo4j.com/docs/operations-manual/current/tutorial/neo4j-admin-import/) in Neo4j 4+. However, due to technical difficulties with using this tool (the import worked, but the graph still did not see the newly created nodes/relationships), this approach was put on the backburner and the [Neo4j-Python driver](https://github.com/neo4j/neo4j-python-driver) was used instead.

The script `create_graph.py` performs the task of starting a Neo4j connection, parsing the CSV and populating the graph using Cypher queries. Although this approach is not as fast/efficient as the `neo4j-admin` tool, it can be further optimized through appropriate use of indexes on key properties for nodes/relationships, as well as through batch transactions with the Neo4j backend.

Run the graph building script as follows:

```
$ python3 create_graph.py
```
The graph, once built, can be queried using Cypher through the Neo4j browser.

The following schema exists for the graph:
#### Node types:
* `App`
* `Org`
* `AHD`

### Relationship types:
* `(:App) -[:USED_BY]-> (:Org)`
* `(:App) -[:IS_SIMILAR_TO]-> (:App)`
* `(:App) -[:HITS]-> (:AHD)`

## Useful Cypher queries

The following queries can be run in Neo4j to retrieve useful data from the graph.

```cql
// Which apps are the most similar to each other?
MATCH (app1:App) -[r:IS_SIMILAR_TO]-> (app2:App)
WHERE r.similarityConnectedComp > 0.95
RETURN app1.PERSID AS app1, app2.PERSID AS app2, r.similarityConnectedComp AS similarity
ORDER BY similarity DESC LIMIT 10
```

```cql
// Top-used apps in the network
MATCH (org:Org) <-[:USED_BY]- (app:App)
RETURN app.PERSID AS appID, SIZE(COLLECT(org.APP_PERSID)) AS numUsed
ORDER BY numUsed DESC LIMIT 5
```

```cql
// Find orgIDs connected to an App PERSID
MATCH x = (app:App) -[:USED_BY]-> (:Org)
WHERE app.PERSID = 427
RETURN x LIMIT 25
```

```cql
// Find AHDs that are hit by apps with a high degree of similarity (> 0.95)
MATCH q = (:AHD) <-[:HITS]- (:App) -[r:IS_SIMILAR_TO]- (:App) -[:HITS]-> (:AHD)
WHERE r.similarityConnectedComp > 0.95
RETURN q LIMIT 50
```

```cql
// Which AHD has the largest number of org sessions connected to it?
MATCH (ahd:AHD) <-[:HITS]- (app:App) -[r:USED_BY]-> (:Org)
RETURN ahd.name AS AHD_Name, app.PERSID AS appID, SIZE(COLLECT(r)) AS numOrgs
ORDER BY numOrgs DESC LIMIT 10
```
