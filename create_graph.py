"""
Script that uses the Neo4j-Python connector to populate a graph DB from CSV files. 

NOTE: This script is not optimized -- further improvements in performance can be obtained
through appropriate use of indexing/constraints and batching the transactions.
"""
import os
from neo4j import GraphDatabase
from neo4j.work.transaction import Transaction
import pandas as pd
from tqdm import tqdm
from typing import Dict, Union


class Neo4jConnection:
    def __init__(
        self, uri: str, user: str, password: str, filenames: Dict[str, str]
    ) -> None:
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.filenames = filenames

    def close(self) -> None:
        self.driver.close()

    def run(self) -> None:
        with self.driver.session() as session:
            session.write_transaction(self._create_indexes)
            session.write_transaction(self._create_app_nodes, self.filenames["apps"])
            session.write_transaction(
                self._create_org_nodes_and_rels, self.filenames["orgs"]
            )
            session.write_transaction(
                self._create_ahd_nodes_and_rels, self.filenames["ahds"]
            )
            session.write_transaction(
                self._create_similarity_connectedcomps_rels,
                self.filenames["similarity_connectedcomps"],
            )

    @staticmethod
    def _create_indexes(tx: Transaction) -> None:
        "Set indexes to improve performance of adding nodes as the graph gets larger"
        index_queries = [
            "CREATE INDEX app_id IF NOT EXISTS FOR (app:App) ON (app.PERSID) ",
            "CREATE INDEX org_id IF NOT EXISTS FOR (org:Org) ON (org.APP_PERSID) ",
            "CREATE INDEX ahd_id IF NOT EXISTS FOR (ahd:AHD) ON (ahd.APP_PERSID) ",
        ]
        for query in index_queries:
            tx.run(query)

    @staticmethod
    def _create_app_nodes(tx: Transaction, filename: str) -> None:
        apps = read_with_nulls(filename)
        fields = list(apps.columns)
        print(f"Adding app nodes from `{filename}`")
        for item in tqdm(apps.itertuples(index=False), total=len(apps)):
            data = dict(zip(fields, item))
            tx.run(
                """
                UNWIND $data as fields
                MERGE (app:App {PERSID: fields.PERSID})
                  SET app += fields
                """,
                data=data,
            )

    @staticmethod
    def _create_org_nodes_and_rels(tx: Transaction, filename: str) -> None:
        orgs = read_with_nulls(filename)
        fields = list(orgs.columns)
        print(f"Adding organization nodes and relationships from `{filename}`")
        for item in tqdm(orgs.itertuples(index=False), total=len(orgs)):
            data = dict(zip(fields, item))
            tx.run(
                """
                UNWIND $data as fields
                MATCH (app:App {PERSID: fields.APP_PERSID})
                MERGE (org:Org {CMDB_Name: fields.CMDB_Name})
                  SET org += fields
                MERGE (app) -[r:USED_BY]-> (org)
                """,
                data=data,
            )

    @staticmethod
    def _create_ahd_nodes_and_rels(tx: Transaction, filename: str) -> None:
        ahds = read_with_nulls(filename)
        fields = list(ahds.columns)
        print(f"Adding AHD-hits nodes and relationships from `{filename}`")
        for item in tqdm(ahds.itertuples(index=False), total=len(ahds)):
            data = dict(zip(fields, item))
            tx.run(
                """
                UNWIND $data as fields
                MATCH (app:App {PERSID: fields.APP_PERSID})
                MERGE (ahd:AHD {name: fields.Name})
                  SET ahd.PERSID = fields.PERSID 
                MERGE (app) -[r:HITS]-> (ahd)
                  SET r.nHits = fields.AHDhits
                """,
                data=data,
            )

    @staticmethod
    def _create_similarity_connectedcomps_rels(tx: Transaction, filename: str) -> None:
        similarities = read_with_nulls(filename)
        fields = list(similarities.columns)
        print(f"Adding similarity (connected comps) relationships from `{filename}`")
        for item in tqdm(similarities.itertuples(index=False), total=len(similarities)):
            data = dict(zip(fields, item))
            tx.run(
                """
                UNWIND $data as fields
                MATCH (app1:App {PERSID: fields.PERSID_1})
                MATCH (app2:App {PERSID: fields.PERSID_2})
                MERGE (app1) -[r:IS_SIMILAR_TO]-> (app2)
                  SET r.similarityConnectedComp = fields.similaritybertcomp, r.compID = fields.CompID
                """,
                data=data,
            )


def read_with_nulls(filepath: str, skiprows: Union[None, int] = None) -> pd.DataFrame:
    """Read in CSV as a pandas DataFrame and fill in NaNs as empty strings"""
    df = pd.read_csv(filepath, sep=",", skiprows=skiprows).fillna("")
    return df


if __name__ == "__main__":
    # List of CSVs that we want to use to populate the DB
    filenames = {
        "apps": "20210330_cmdb_ci_business_app_V2_noDescription.csv",
        "orgs": "20210401-AccessIT-APPLICATIONS-ORGANIZATIONS-reduced_CMDB_exact_matches.csv",
        "ahds": "20210517-CMDB-AHD-hits.csv",
        "similarity_connectedcomps": "20210719_cmdb_similarities_sentencebert_08_threshold_conntected_components.csv",
    }
    # Path to clean data CSVs
    data_path = "graph_data_clean"
    filenames = {key: os.path.join(data_path, val) for key, val in filenames.items()}
    # Start connection and run build queries
    connection = Neo4jConnection(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="12345",
        filenames=filenames,
    )
    connection.run()
    connection.close()
