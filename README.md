# Joern CPG to Neo4j

This repository contains code to import CPG extracted from Joern (neo4jcsv format) into Neo4j.

First, install Joern/CPG (https://github.com/joernio/joern) -> `joern-parse /src/directory` -> `joern-export --out outdir --repr=all --format=neo4jcsv` (https://docs.joern.io/export/)

**Note**: Please refer to the [Joern documentation](https://docs.joern.io) for more information on how to use Joern.

## Usage

1. Use Python 3.10 or newer, then create and activate a virtual environment:
   
   `python3 -m venv .venv`

   `source .venv/bin/activate`

2. Install dependencies from `requirements.txt`:
   
   `pip install -r requirements.txt`

3. Create `config.yml` based on `config_example.yml`, then update it with the path to the csv files and the Neo4j database information.

4. Start your Neo4j database.

5. Run the `main.py` file to import the CPG into Neo4j.

## Alt usage

If you're importing nodes and edges from a large project, importing via Cypher, as suggested by Joern, can be very slow. For a significant speedup, you can use the Neo4j data import instructions - https://neo4j.com/docs/operations-manual/current/import/#import-tool-examples .

1. If necessary, modify the import parameters, as well as the nodes and edges, in the `import_args.txt` file. (The file specifies nodes and edges for Joern/CPG 2.0.1, and you may need to add/remove entries if Joern exports other types.)
   
2. In the directory with your Neo4j database (example directory for Linux: `/home/user/.config/neo4j-desktop/Application/Data/dbmss/dbms-fb8e0921-28fd-4ce9-8bee-1e7862e47bf1`), create the `import/` folder and move all the CSV files into it (or change the path to the CSV files in the `import_args.txt` file).
   
3. Run the command `HEAP_SIZE=3G ./bin/neo4j-admin @import_args.txt` in the directory with your Neo4j database. (The database must be stopped while executing the command.)

## Dependencies

- neo4j (https://pypi.org/project/neo4j/)
- pyyaml (https://pypi.org/project/pyyaml/)

## Contributing

Please feel free to open an issue or a pull request if you find any bugs, have any questions or want to make any suggestions.
