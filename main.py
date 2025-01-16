from flask import Flask, request, jsonify
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
server_url = os.getenv("NEO4J_SERVER_URL")
API_KEY = os.getenv("API_KEY")  # Store your API key in an environment variable

class Neo4jSchema:
    def __init__(self, url):
        self.driver = GraphDatabase.driver(url, encrypted=False)

    def get_schema(self):
        with self.driver.session() as session:
            # Query to get relationship types and their properties along with from-node and to-node labels
            rel_types_query = """
            CALL db.schema.relTypeProperties()
            YIELD relType, propertyName, propertyTypes, mandatory
            RETURN relType, propertyName, propertyTypes, mandatory
            """
            
            # Query to get all node labels
            node_labels_query = """
            CALL db.labels()
            YIELD label
            RETURN label
            """

            # Execute the relationship types query
            rel_types_result = session.run(rel_types_query)
            relationships = []
            for record in rel_types_result:
                relationships.append({
                    "relationship": record["relType"],
                    "property": record["propertyName"],
                    "propertyTypes": record["propertyTypes"],
                    "mandatory": record["mandatory"]
                })

            # Execute the node labels query
            node_labels_result = session.run(node_labels_query)
            nodes = [record["label"] for record in node_labels_result]

            return {
                "nodes": nodes,
                "relationships": relationships
            }

# Initialize Flask app
app = Flask(__name__)

# Create a Neo4jSchema instance
neo4j_schema = Neo4jSchema(
    url=f"bolt://{server_url}:7687",
)

def api_key_required(f):
    """Decorator to require API key for access."""
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get('X-API-Key')
        if api_key != API_KEY:
            return jsonify({"error": "Unauthorized access"}), 403
        return f(*args, **kwargs)
    return decorated_function

@app.route('/schema', methods=['GET'])
@api_key_required  # Protect this route with the API key requirement
def schema():
    try:
        schema_info = neo4j_schema.get_schema()
        return jsonify(schema_info), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Google Cloud Function entry point
def cloud_function(request):
    """HTTP Cloud Function."""
    with app.request_context(request.environ):
        return app.full_dispatch_request()

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
