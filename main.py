from cpg_to_neo4j import cpgToNeo4j
import yaml

with open("config.yaml", "r") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)


# Pipeline: From CPG to Neo4j
pipe = cpgToNeo4j(
    config.get("neo4j_uri"),
    config.get("neo4j_user"),
    config.get("neo4j_password"),
)

pipe.copy_data_to_neo4j_import_folder(
    config.get("export_path"),
    config.get("neo4j_import_path")
)
pipe.upload_nodes(
    config.get("export_path")
)
pipe.upload_edges(
    config.get("export_path")
)
