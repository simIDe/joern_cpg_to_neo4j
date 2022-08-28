from neo4j_connection import Neo4jConnection
import shutil
import os


class cpgToNeo4j(Neo4jConnection):
    """Children class of Neo4jConnection for create a CPG to Neo4j pipeline
    """

    def __init__(self, uri, user, pwd):
        """Constructor of the class.

        Args:
            uri (str): URI of the neo4j database.
            user (str): Neo4j Username 
            pwd (str): Password of the neo4j database.
        """
        super().__init__(uri, user, pwd)

    def upload_nodes(self, folder_path):
        """Execute neo4j query contained in each file with 'node' in their name

        Args:
            path (str): Path to the folder containing the files.
        """
        # Get all files in the folder
        files = [f for f in os.listdir(folder_path) if os.path.isfile(
            os.path.join(folder_path, f))]
        # keep only the files with 'node' and 'cypher' in their name
        files = [f for f in files if 'node' in f and 'cypher' in f]
        # change working directory to the folder
        os.chdir(folder_path)
        # execute query for each file
        for file in files:
            print(f"Executing query from file {file}")
            with open(file, 'r') as f:
                query = f.read()
            self.query(query)
            print(f"Query from file {file} executed")

    def upload_edges(self, folder_path):
        """Execute neo4j query contained in each file with 'edge' in their name

        Args:
            path (str): Path to the folder containing the files.
        """
        # Get all files in the folder
        files = [f for f in os.listdir(folder_path) if os.path.isfile(
            os.path.join(folder_path, f))]
        # keep only the files with 'edge' and 'cypher' in their name
        files = [f for f in files if 'edge' in f and 'cypher' in f]
        # change working directory to the folder
        os.chdir(folder_path)
        # execute query for each file
        for file in files:
            print(f"Executing query from file {file}")
            with open(file, 'r') as f:
                query = f.read()
            self.query(query)
            print(f"Query from file {file} executed")

    def copy_data_to_neo4j_import_folder(self, data_folder, neo4j_import_folder):
        """Copy data from a folder to a neo4j import folder.

        Args:
            data_folder (str): Folder containing the data to be copied.
            """
        # Get all files in the folder
        data_files = [f for f in os.listdir(
            data_folder) if os.path.isfile(os.path.join(data_folder, f))]
        # Get all files in the neo4j import folder
        neo4j_files = [f for f in os.listdir(neo4j_import_folder) if os.path.isfile(
            os.path.join(neo4j_import_folder, f))]
        # Check if the files in the neo4j import folder are the same as the files in the data folder
        if not set(data_files) == set(neo4j_files):
            print(f"Copying files from {data_folder} to {neo4j_import_folder}")
            for file in data_files:
                shutil.copy(os.path.join(data_folder, file),
                            os.path.join(neo4j_import_folder, file))

            print(f"Files copied")
        else:
            pass
            print("Files are already up to date")
