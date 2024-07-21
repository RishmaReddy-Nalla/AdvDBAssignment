from flask import Flask, jsonify, request
from neo4j import GraphDatabase
import atexit


app = Flask(__name__)
uri = "neo4j+ssc://41236159.databases.neo4j.io:7687"
auth = ("rishma", "Reddy~1235")

with GraphDatabase.driver(uri, auth=auth) as driver:
    driver.verify_connectivity()
    print("connected!")

def get_session():
    return driver.session()


def close_driver():
    if driver is not None:
        driver.close()

atexit.register(close_driver)

@app.route("/create", methods=['POST'])
def add_node():
    # Get the JSON data from the request
    data = request.get_json()

    # Check if data is provided
    if data is None:
        return jsonify({"error": "No data provided!"}), 400

    # Extract the label and properties from the data
    label = data.get("label")
    properties = data.get("properties", {})

    # Check if label is missing
    if not label:
        return jsonify({"error": "Missing node label!"}), 400

    try:
        # Call the create_node function to create the node
        create_node(label, properties)
        return jsonify({"message": "Node Created Successfully"}), 201
    except Exception as e:
        return jsonify({"error": f"{str(e)}"}), 500

# Define a function to create a node in the Neo4j database
def create_node(label, properties):
    with get_session() as session:
        session.write_transaction(insert_record, label, properties)

# Define a function to insert a record into the Neo4j database
def insert_record(tx, label, properties):
    # Construct the Cypher query to create a node with the given label and properties
    query = f"CREATE (n:{label} {{"
    query += ", ".join([f"{key}: ${key}" for key in properties.keys()])
    query += "})"
    # Execute the query with the provided properties
    tx.run(query, **properties)



@app.route("/fetch_nodes/<node_name>/<int:limit>", methods=['GET'])
def read_node(node_name, limit):
    # Construct the Cypher query to fetch nodes of a specific label with a limit
    query = f'''
            MATCH (n:{node_name})
            RETURN n
            limit {limit}
            '''
    with get_session() as session:
        result = session.run(query)
        nodes = []
        for record in result:
            node_dict = {}
            for key in record['n'].keys():
                node_dict[key] = record['n'][key]
            nodes.append(node_dict)
        return jsonify(nodes)



# Define a route for deleting a node
@app.route("/delete", methods=['POST', 'DELETE'])
def remove_node():
    # Get the JSON data from the request
    data = request.json
    label = data.get("label")
    properties = data.get("properties")

    # Check if label and properties are provided
    if not label or not properties:
        return jsonify({"error": "Label and properties are required"}), 400

    try:
        # Call the delete_node function to delete the node
        delete_node(label, properties)
        return jsonify({"message": "Node deleted successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# Define a function to delete a node in the Neo4j database
def delete_node(label, properties):
    with get_session() as session:
        session.write_transaction(delete_record, label, properties)

# Define a function to delete a record from the Neo4j database
def delete_record(tx, label, properties):
    # Construct the Cypher query to match and delete the node with the given label and properties
    query = f"MATCH (n:{label} {{"
    query += ", ".join([f"{key}: ${key}" for key in properties.keys()])
    query += "}) DELETE n"
    # Execute the query with the provided properties
    tx.run(query, **properties)


# Define a route for updating a node
@app.route("/update", methods=['POST', 'UPDATE'])
def update():
    # Get the JSON data from the request
    data = request.json
    label = data.get("label")
    match_properties = data.get("match_properties")
    update_properties = data.get("update_properties")

    # Check if label, match properties, and update properties are provided
    if not label or not match_properties or not update_properties:
        return jsonify({"error": "Label, match properties, and update properties are required"}), 400

    try:
        # Call the update_node function to update the node
        update_node(label, match_properties, update_properties)
        return jsonify({"message": "Node updated successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Define a function to update a node in the Neo4j database
def update_node(label, match_properties, update_properties):
    with get_session() as session:
        session.write_transaction(update_record, label, match_properties, update_properties)

# Define a function to update a record in the Neo4j database
def update_record(tx, label, match_properties, update_properties):
    # Construct the Cypher query to match the node with the given label and match properties,
    # and update the node with the given update properties
    match_query = f"MATCH (n:{label} {{"
    match_query += ", ".join([f"{key}: ${key}" for key in match_properties.keys()])
    match_query += "}) "

    set_query = "SET "
    set_query += ", ".join([f"n.{key} = ${key}" for key in update_properties.keys()])

    query = match_query + set_query

    # Execute the query with the provided match properties and update properties
    tx.run(query, **{**match_properties, **update_properties})



@app.route("/get_nodes/<label>/<property>/<value>", methods=['GET'])
def get_node(label, property, value):
    # Construct the Cypher query to fetch nodes of a specific label with a specific property value
    query = f"""
            MATCH (n:{label})
            WHERE n.{property} = $value
            RETURN n
            """
    parameters = {"value": value}
    with get_session() as session:  
        result = session.run(query, parameters)
        # Convert the result into a list of dictionaries
        result = [record.data() for record in result]
    return jsonify(result)


# Define the root route of the Flask API
@app.route('/')
def index():
    return "Welcome to the Neo4j Flask API"

# Run the Flask app if the script is executed directly
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5555, debug=True)