import json
from sortedcontainers import  SortedSet, SortedDict
from arango import ArangoClient
from datetime import datetime
import sys, traceback
import configparser
import os
from pathlib import Path
from arango.http import DefaultHTTPClient
import glob

def YesNo():

  yes_choices = ['yes', 'y']
  no_choices = ['no', 'n']

  while True:
    user_input = input('\nDo you want to proceed (yes/no)? ')
    if user_input.lower() in yes_choices:
        print('You typed yes\n')
        return "YES"
    elif user_input.lower() in no_choices:
        print('You typed no\n')
        return "NO"
    else:
        print('Please type yes or no\n')



config = configparser.ConfigParser()

if not Path("config.ini").is_file():
    print("FATAL: Missing config file")
    exit(1)

config.read("config.ini")

# Validate config
# for section in config.sections():
#   print(f"Section: {section}")
#   for item in config.items(section):
#     print(item)

# exit(1)

#Arango Database defaults
ENDPOINTS = config["Database"]["endpoints"].split(",")
USER = config["Database"]["username"]
PASS = config["Database"]["password"]

#Source defaults
FILE = config["Source"]["data_file"]

#Sink defaults
DATABASE = config["Sink"]["database"]
NODE_COLLECTION= config["Sink"]["node_collection"]


#Processing defaults
BATCH_SIZE=config["Processing"].getint("batch_size")
NODE_MODE=config["Processing"].getint("node_mode")



DROP_DATABASE=config["Processing"].getboolean("drop_database")
SKIP_NODES=config["Processing"].getboolean("skip_nodes")
SKIP_EDGES=config["Processing"].getboolean("skip_edges")
SKIP_EDGE_NODES=config["Processing"].getboolean("skip_edge_nodes")

print(f"Config:")
print(f"=======")
print(f"\nDatabase > Endpoints: {ENDPOINTS}")
print(f"Database > username: {USER}")
print(f"Database > password: {PASS}")
print(f"\nSource > data_file: {FILE}")
print(f"\nSink > database: {DATABASE}")
print(f"Sink > node_collection: {NODE_COLLECTION}")
print(f"\nProcessing > batch_size: {BATCH_SIZE}")
print(f"Processing > drop_database: {DROP_DATABASE}")
print(f"Processing > skip_nodes: {SKIP_NODES}")
print(f"Processing > skip_edges: {SKIP_EDGES}")
print(f"Processing > skip_edge_nodes: {SKIP_EDGE_NODES}")

if DROP_DATABASE == True:
  print("\n****** Warning! Configuration cleanup existing data ******")
  print("Drop Database: TRUE (ALL Collections will be dropped)")
  print(f"Database: {DATABASE}\nEndpoint(s): {ENDPOINTS}\n")
  print("\n**********************************************************\n")
  yn=YesNo()

  if yn == "NO":
    exit(1)


def getFileAsList(file):
    data_lines=[]
    with open(file, 'r') as f:
        line = f.readline()
        #print(f"Line is: {line} of type{type(line)}")
        while line:
            data = json.loads(line)
            data_lines.append(data)
            line = f.readline()

    return data_lines



def createEdgeNodes(data):
    nodes=[]

    for type in ["start","end"]:
        new_doc = {}
        key=data[type]["id"]
        new_doc["_key"]=key

        #Statically coding both sides of the relationship
        new_doc["dtype"]="node"

        labels = data[type]["labels"]

        # if "properties" om data["type"]:
        #   new_doc.update(data[type]["properties"])

        #Follow two approaches for now for the labels in Neo4j
        #Figure out which one ends up being more efficient

        #Design #1: Label Name = True
        for l in labels:
            new_doc[l]=True

        #Design #2: Array of all Labels
        new_doc["labels"] = labels

        #Merge all remaining properties
        if "properties" in data[type]:
            new_doc.update(data[type]["properties"])
        else:
            print(f"Warning! Document of type '{new_doc['dtype']}' in RELATIONSHIP '{data['label']}' with id: {key} has no properties")
        nodes.append(new_doc)

    return nodes

def createDocument(data):
    dtype= data["type"]
    key = data["id"]

    new_doc = {}
    new_doc["_key"]=key
    #Only for debugging right now remove later
    new_doc["dtype"]=dtype

    if dtype == "node":
        labels = data["labels"]
        #Follow two approaches for now for the labels in Neo4j
        #Figure out which one ends up being more efficient

        #Design #1: Label Name = True
        for l in labels:
            new_doc[l]=True

        #Design #2: Array of all Labels
        new_doc["labels"] = labels

    else:
        labels = data["label"]
        new_doc["label"] = labels
        new_doc["_from"]= data["start"]["id"]
        new_doc["_to"]= data["end"]["id"]

    #print(f"Edge: {key} Value:\n{json.dumps(new_doc)}")

    #Merge all remaining properties
    if "properties" in data:
        new_doc.update(data["properties"])
    else:
        print(f"Warning! Document of type '{new_doc['dtype']}' in NODES with id: {key} has no properties")
        print(f"Data: {json.dumps(data)}\nNew Doc: {json.dumps(new_doc)}")

    return new_doc


def createGraph(ar):

    #Create graph
    graph_name = f"the_graph"
    print(f"\nCreating graph {graph_name}")
    if db.has_graph(graph_name):
        r = db.graph(graph_name)
    else:
        r = db.create_graph(graph_name)

    # Create an edge definition for edge edge and the known collections
    for rel, from_to in ar.items():
        print(f"Processing: edge > {rel} from >{list(from_to['from'])} to > {list(from_to['to'])}")
        if not r.has_edge_definition(rel):
            acted = r.create_edge_definition(
                edge_collection=rel,
                from_vertex_collections=list(from_to["from"]),
                to_vertex_collections=list(from_to["to"])
            )


def postProcess(db,folder):
    print("\nPost processing files")
    
    path = os.path.join(os.getcwd(),"Data",folder)
    gpath =os.path.join(path,"*.json")

    print(f"PATH: {path}")
    files = [os.path.basename(f) for f in glob.glob(gpath)]
    print(f"Files: {files}")

    for f in files:
        if f.startswith("NODE"):
            if f.startswith("NODES"):
                collectionName = NODE_COLLECTION
            else:
                if NODE_MODE == 2:
                    continue 
                collectionName = f[5:-5]

            #Create required edge and document collections
            if db.has_collection(collectionName):
              col  = db.collection(name=collectionName)
            else:
              print(f"\nDocument Collection '{collectionName}' not found.\nCreating collection")
              col  = db.create_collection(name=collectionName)

            try:
              file = os.path.join(path,f)
              import_list = getFileAsList(file)
              print(f"Found {len(import_list)} documents to import using file {file}")
              result=col.import_bulk(import_list,batch_size=1000000,on_duplicate="update",details=True)
              print(result)
            except:
              print("Exception occured during processing")
              traceback.print_exc(file=sys.stdout)
              tb=traceback.format_exc()
              exit(1)

        elif f.startswith("EDGE"):
            collectionName = f[5:-5]
            
            if db.has_collection(collectionName):
              col_edge  = db.collection(name=collectionName)
            else:
              print(f"\nEdge Collection '{collectionName}' not found.\nCreating edge collection")
              col_edge  = db.create_collection(name=collectionName,edge=True)

            try:
              file = os.path.join(path,f)
              import_list = getFileAsList(file)
              print(f"Found {len(import_list)} edges to import using file {file}")
              result=col_edge.import_bulk(import_list,batch_size=1000000,on_duplicate="update",details=True)
              print(result)
            except:
              print("Exception occured during processing")
              traceback.print_exc(file=sys.stdout)
              tb=traceback.format_exc()
              exit(1)
        else:
            print(f"\nSkipping processing of file -> {f}")

def closeAllFileHandle(fh):
    #Close Open File Handles
    for ne,fh in fh.items():
        print(f"Closing file handle for {ne}")
        fh.close()



## Inititializations
class MyCustomHTTPClient(DefaultHTTPClient):
    REQUEST_TIMEOUT = 240 # Set the timeout you want in seconds here


# Initialize a client
client = ArangoClient(hosts=ENDPOINTS,request_timeout=240)

# Connect to the system database
sys_db = client.db("_system", username=USER, password=PASS)
# Retrieve the names of all databases on the server as list of strings
db_list = sys_db.databases()

if DROP_DATABASE == True and DATABASE in db_list:
  sys_db.delete_database(DATABASE)

if DATABASE not in db_list or DROP_DATABASE == True:
  print(f"\nCreating Database '{DATABASE}'")
  sys_db.create_database(DATABASE)

# Connect to DATABASE  as user.
db = client.db(DATABASE, username=USER, password=PASS)


## End Initializations

num_nodes=0
num_edges=0
num_edges_nodemode=0
num_relationship_nodes=0

edge_collections={}
node_collections={}

#Edge: {From: [] From: To[]} 
all_relationships = {}

files_created={}
folder_name=None


## Template "<Input File Name>_YYYY-MM-DD_HH.MM.SS"
folder = os.path.join(os.getcwd(), FILE.split(".json")[0]+ "_" + datetime.now().strftime('%Y-%m-%d_%H.%M.%S'))
os.makedirs(folder)


# ##Debugging only
INGEST_ONLY=False
# folder = "recommendations_2024-03-23_11.55.20"
# ###



if INGEST_ONLY == False:
    if NODE_MODE == 2 or NODE_MODE == 3:
        files_created[f"NODES"]=open(os.path.join(folder, f"NODES.json"), 'w')


    #with open(os.path.join(mydir, 'filename.txt'), 'w') as d:

    with open(FILE, 'r') as f:
        line = f.readline()
        while line:
        #Edge or Node document created here
            data = json.loads(line)

            #Algo
            #1) Create a file for each Node label NODE_LABEL.csv
            #2) Create a file for each Edge Label EDGE_LABEL.csv (Edge can have only one label)
            #3) Create a file for each Node in a relationship ENODE_LABEL.csv (One From and One To but each with Multiple Labels)
            #3) Write line / dict to each file 
            # Note: Depending upon the Node Mode write the _from / _to for the edge node


            if data['type'] == "node":
                #Skip ingesting nodes
                if SKIP_NODES == True:
                    #Next line
                    line = f.readline()
                    continue

                new_doc = createDocument(data)
                key = new_doc['_key']

                print(f"Processing NODE with id: {new_doc['_key']}")

                for l in new_doc["labels"]:
                    
                    if NODE_MODE == 3 or NODE_MODE ==2: 
                        fh_n = files_created[f"NODES"]
                        fh_n.write(json.dumps(new_doc)+"\n")

                    if NODE_MODE ==1 or NODE_MODE ==3:
                        #Create a file handle to write to the NODE with specific label
                        if f"NODE_{l}" not in files_created:
                            files_created[f"NODE_{l}"]=open(os.path.join(folder, f"NODE_{l}.json"), 'w')
                        
                        fh_n = files_created[f"NODE_{l}"]
                        fh_n.write(json.dumps(new_doc)+"\n")



                num_nodes = num_nodes + 1
            elif data['type'] == "relationship":

                #Skip EDGES and NODES if SKIP_EDGES is True
                if SKIP_EDGES == True:
                    #Next line
                    line = f.readline()
                    continue

                relationship = createDocument(data)
                rkey = relationship['_key']

                print(f"Processing RELATIONSHIP '{relationship['label']}' with id: {relationship['_key']}, start: {relationship['_from']}, end: {relationship['_to']}")

                num_edges = num_edges + 1

                if SKIP_EDGE_NODES == False:
                    data = json.loads(line)
                    nodes=createEdgeNodes(data)


                    for enode in nodes:
                        key = enode['_key']

                        print(f"Processing ENODE with id: {enode['_key']}")

                        for l in enode["labels"]:
                            if f"ENODE_{l}" not in files_created:
                                files_created[f"ENODE_{l}"]=open(os.path.join(folder, f"ENODE_{l}.json"), 'w')
                        
                            fh_en = files_created[f"ENODE_{l}"]
                            fh_en.write(json.dumps(enode)+"\n")


                    num_relationship_nodes = num_relationship_nodes + 2



                    #Add Edges for the from Node

                    print(f"Processing Edge/Relationship with id: {rkey}")
                    el = data["label"]
                    if f"EDGE_{el}" not in files_created:
                        files_created[f"EDGE_{el}"]=open(os.path.join(folder, f"EDGE_{el}.json"), 'w')
                    

                    fh_e = files_created[f"EDGE_{el}"]

                    if NODE_MODE == 1 or NODE_MODE ==3:
                        for ls in nodes[0]["labels"]:
                            for le in nodes[1]["labels"]:
                                #Changed id to _key
                                edge={"type":"relationship", "_key": f"{rkey}-{ls}-{le}", "label": data["label"],"_from": f"{ls}/{data['start']['id']}", "_to": f"{le}/{data['end']['id']}"}

                                if el not in all_relationships:
                                    all_relationships[el] = {"from":set(),"to": set()}

                                #Add from and to vertex collections
                                all_relationships[el]["from"].add(ls)
                                all_relationships[el]["to"].add(le)


                                fh_e.write(json.dumps(edge)+"\n")
                                num_edges = num_edges + 1

                    if NODE_MODE ==2 or NODE_MODE ==3:
                        #Changed id to _key
                        edge={"type":"relationship", "_key": f"{rkey}", "label": data["label"],"_from": f"{NODE_COLLECTION}/{data['start']['id']}", "_to": f"{NODE_COLLECTION}/{data['end']['id']}"}

                        if el not in all_relationships:
                            all_relationships[el] = {"from":set(),"to": set()}

                        #Add from and to vertex collections
                        all_relationships[el]["from"].add(NODE_COLLECTION)
                        all_relationships[el]["to"].add(NODE_COLLECTION)
                                
                        fh_e.write(json.dumps(edge)+"\n")

            #Next line
            line = f.readline()

    #Close all file handles, not doing this results in partially read or empty files
    closeAllFileHandle(files_created)


    #Post process all the written files, ingest into DB
    postProcess(db,folder)


    #Create graphs
    createGraph(all_relationships)



#batch_db.commit()
print(f"\n\nSummary:\nInserted {num_nodes} nodes as raw nodes \nInserted {num_relationship_nodes} nodes via relationships\nInserted {num_edges} as edges")
print("\n\nDone!\n")
