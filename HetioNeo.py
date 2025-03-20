#Neo4j path:   "C:\Users\young\AppData\Local\Programs\Neo4j Desktop\Neo4j Desktop.exe"
from py2neo import Graph, Node, Relationship #Node used bc cypher LOAD issues
import pandas as pd

URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "Abcdef12345"
graph = Graph(URI, auth = (USERNAME,PASSWORD)) #driver and connection established

cypherCommands = []

df = pd.read_csv('nodes.tsv', delimiter='\t')

#######################################################df tampering#####################################
# Display first 5 rows of the DataFrame
# print("\n=== First 5 rows of nodes DataFrame ===")
# print(df.head())

# # Display basic information about the DataFrame
# print("\n=== DataFrame Info ===")
# print(df.info())

# # Display basic statistics of the DataFrame
# print("\n=== DataFrame Description ===")
# print(df.describe())

# # Display column names
# print("\n=== DataFrame Columns ===")
# print(df.columns.tolist())

# # Display the shape (rows x columns) df.shape = (rows, columns)
# print("\n=== DataFrame Shape ===")   #gives the matrix dimensions (entries x columns)
# print(f"Number of rows: {df.shape[0]}")
# print(f"Number of columns: {df.shape[1]}")

# # Example of iterrows() for nodes
# print("\n=== Example of iterrows() for nodes (first 3 rows) ===")
# for index, row in df.iterrows():
#     if index < 3:  # Only show first 3 rows
#         print(f"\nRow {index}:")  #Index shows row number (not including headers) in spreadsheet
#         print(f"Index: {index}")
#         print(f"Row type: {type(row)}")
#         print(f"Row contents:")
#         print(f"  id: {row['id']}") #to access a partiuclar cell/entry of the ss, use row[<columnid of interest>]
#         print(f"  name: {row['name']}")
#         print(f"  kind: {row['kind']}")
#         print("-" * 50)

################################################Cypher Commands###############################################
# sample  
# create = """CREATE (a:Person {name: 'Young', age:30}) 
# RETURN a
# """

# clear = "MATCH (n) DETACH DELETE n"


# # CAREFUL: CLEARS THE DB!!!!!!
# try:
#     print("Clearing database for next run...")
#     graph.run(clear)

# except Exception as e:
#     print(f"Clear Error: {e}")


nodesDict = {} 
##########################################(~L70-160 node,edge creation)###########################
#load data into neo4j:   
# try:   
#     print("Loading nodes into Neo4j...")
#     for _, row in df.iterrows():
#         nodeId = row['id']
#         if nodeId not in nodesDict:
#             # Split the ID to get the type and actual identifier
#             id_parts = nodeId.split("::") # [Anatomy, UBERON:0000002]
#             node_type = id_parts[0]  # Anatomy, Gene, Disease, or Compound
#             id_tail = "::".join(id_parts[1:])  # id tail 
            
#             # Create node with proper label and properties
#             node = Node(node_type,  # Use node_type as the node's label
#                        full_id=nodeId,  # full id
#                        id=id_tail,   # idTail is main part of id
#                        name=row['name'])
            
#             # Create indexes if they don't exist (do this only once)
#             if nodeId == df['id'].iloc[0]:  # Create index on node label 'node_type' on its 'full_id' and 'name' properties
#                 graph.run(f"CREATE INDEX IF NOT EXISTS FOR (n:{node_type}) ON (n.full_id)")
#                 graph.run(f"CREATE INDEX IF NOT EXISTS FOR (n:{node_type}) ON (n.name)")
            
#             graph.create(node)
#             nodesDict[nodeId] = node
#     print("Nodes loaded successfully!")

# except Exception as e:
#     print(f"Loading from df Error: {e}") 


# loadTsv = """LOAD CSV WITH HEADERS FROM 'file:///C:/Users/young/OneDrive/Desktop/Project1_BD/sample_nodes.tsv' AS line
#              FIELDTERMINATOR '\t'
#              CREATE (:Node {id: line.id, name: line.name, kind: line.kind}) """


# #ERROR neo4j match unit error
# # try:
# #     print("attempting to load tsv: ")
# #     graph.run(loadTsv)
# #     print("load successful!")

# # except Exception as e:
# #     print(f"loading error: {e}")

# ################################################load Edges###############################################

# dfe = pd.read_csv('edges.tsv', delimiter='\t')

# # Example of iterrows() for edges
# print("\n=== Example of iterrows() for edges (first 3 rows) ===")  
# for index, row in dfe.iterrows():
#     if index < 3:  # Only show first 3 rows
#         print(f"\nRow {index}:")
#         print(f"Index: {index}")
#         print(f"Row type: {type(row)}")
#         print(f"Row contents:")            #row is a dictionary, dfe.iterrrows() contains multiple rows, accessed with index 
#         print(f"  source: {row['source']}")#row['source'] = Disease::DOID: .... 
#         print(f"  target: {row['target']}")
#         print(f"  metaedge: {row['metaedge']}")
#         print("-" * 50)

# try: #Extract edge fields
#     print("Creating edges in Neo4j...")
#     for _, row in dfe.iterrows():
#         source_id = row['source'] #eg) Disease::DOID: .... 
#         target_id = row['target'] #eg) Gene::HGNC: .... 
#         relation = row['metaedge'] #eg) "AeG"
        
#         # If both source and target nodes exist in dictionary, then an edge (relation) exists between them
#         if source_id in nodesDict and target_id in nodesDict:
#             source_node = nodesDict[source_id]
#             target_node = nodesDict[target_id]
            
#             # Create relationship in Neo4j
#             relationship = Relationship(source_node, relation, target_node)
#             graph.create(relationship)
            
#           #Testing: #edge creation successful
#             #print(f"Created relationship: {source_id} -[{relation}]-> {target_id}") 
#         else: #Testing: failed edges
#             print(f"Warning: missing nodes for relationship: {source_id} -[{relation}]-> {target_id}")
    
#     print("Edges created successfully!")

# except Exception as e:
#     print(f"Error creating edges: {e}") 



###########################################UI: get query from user####################################################################
while True:
    userCommand = input("""Type:
    1) 'disease' to get disease information
    2) 'edges' to get all edge types and their respective counts 
    3) 'new' to retrieve all compounds that treat a new disease 
    4) 'quit' to quit
    >>> """)

    if userCommand.lower() == 'quit':
        print("Goodbye! ")
        break
   
    try: #edge type and count
        if userCommand.lower() == 'edges':
            # Show edge statistics
            result = graph.run("MATCH ()-[r]->() RETURN type(r), count(r)") #return all distinct node types with their respective counts
            print("\nEdge types and counts:")
            for record in result:
                print(f"Type: {record['type(r)']}|Count: {record['count(r)']}")

#QUERY 1, given disease ID, output stats (Compounds that directly treat/palliate Disease, Genes that cause Disease (all three), location of disease (anatomy))
        elif userCommand.lower() == "disease":  
            disease_id = input("Enter disease ID (ex: 'Disease::DOID:12365'): ")
            query = f"""
            MATCH (d:Disease {{full_id: '{disease_id}'}}) 
            OPTIONAL MATCH (c:Compound)-[:CtD]->(d)    //#OPTIONAL MATCH since queries are pipelined (in case of empty return) 
            OPTIONAL MATCH (g:Gene)<-[:DdG|:DuG|:DaG]-(d)
            OPTIONAL MATCH (a:Anatomy)<-[:DlA]-(d)
            RETURN DISTINCT
                d.name as Disease,
                collect(DISTINCT c.name) as `Treating/Palliating Compounds`,
                collect(DISTINCT g.name) as `Causal Genes`,
                collect(DISTINCT a.name) as Locations
            """  
             
            print(f"Executing query with disease_id: {disease_id}")
            result = graph.run(query)
    
    # Convert result to list (empty-> invalid disease id)
            results_list = list(result)
            print(f"Found: {len(results_list)}")
    
            if len(results_list) == 0:
                print("No disease found with that ID, exiting...\n")
            else:
                for record in results_list:
                    print("Disease Details")
                    print("-" * 100)
                    print(f"Name: {record['Disease']}")
                    print(f"Treating/Palliating Compounds: {record['Treating/Palliating Compounds']}\n")
                    print(f"Causal Genes: {record['Causal Genes']}\n")
                    print(f"Locations: {record['Locations']}")
                    print("-" * 100)
            
#QUERY 2: Print all Compounds that newly treat a disease (PRE: 'palliates' is a form of 'treat')
        elif userCommand.lower() == "new":
            query = """ 
        //Pattern 1: Compound upregulates, Anatomy downregulates w/o (treats/palliates) edges
            MATCH path1 = (c:Compound)-[:CuG]->(g:Gene)<-[:AdG]-(a:Anatomy)<-[:DlA]-(d:Disease)
            WHERE NOT EXISTS ((c)-[:CtD]->(d)) AND NOT EXISTS ((c)-[:CpD]->(d))
            RETURN DISTINCT c.name as Compound            

            UNION
        //Pattern 2: Compound downregulates, Anatomy upregulates
            MATCH path2 = (c:Compound)-[:CdG]->(g:Gene)<-[:AuG]-(a:Anatomy)<-[:DlA]-(d:Disease)
            WHERE NOT EXISTS ((c)-[:CtD]->(d)) AND NOT EXISTS ((c)-[:CpD]->(d)) 
            RETURN DISTINCT c.name as Compound            
            """

            result = graph.run(query) 
            print("\nNewly treating compounds found:")
            for record in result:
                print(f"{record['Compound']}")
              
                    
    except Exception as e:
        print(f"Invalid command: {e}, try again!") 


# Sample cypher to run 
# output = graph.run(create) #run query

# #output the result
# for x in output:
#     print(x)


# CAREFUL: CLEARS THE DB
# try:
#     print("Clearing database for next run...")
#     graph.run(clear)

# except Exception as e:
#     print(f"Clear Error: {e}")

