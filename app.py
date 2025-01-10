from neo4j import GraphDatabase
from neo4j.exceptions import CypherSyntaxError
import openai
from dotenv import load_dotenv
import os
from queries import *

# Load environment variables from .env file
load_dotenv()
openai_key = os.getenv("OPENAI_API_KEY")
server_url = os.getenv("NEO4J_SERVER_URL")

def schema_text(node_props, rel_props, rels):
    return f"""
  This is the schema representation of the Neo4j database.
  Node properties are the following:
  {node_props}
  Relationship properties are the following:
  {rel_props}
  Relationship point from source to target nodes
  {rels}
  Make sure to respect relationship types and directions
  """

class Neo4jGPTQuery:
    def __init__(self, url, openai_api_key):
        self.driver = GraphDatabase.driver(url, encrypted=False)
        openai.api_key = openai_api_key
        # construct schema
        self.schema = self.generate_schema()

    def generate_schema(self):
        node_props = self.query_database(node_properties_query)
        rel_props = self.query_database(rel_properties_query)
        rels = self.query_database(rel_query)
        return schema_text(node_props, rel_props, rels)

    def refresh_schema(self):
        self.schema = self.generate_schema()

    def get_system_message(self):
        return f"""
        Task: Generate Cypher queries to query a Neo4j graph database based on the provided schema definition.
        Instructions:
        Use only the provided relationship types and properties, maintaining the same node names and relationships throughout your responses.
        Do not use any other relationship types or properties that are not provided.
        If you cannot generate a Cypher statement based on the provided schema, explain the reason to the user.
        Schema:
        {self.schema}

        Note: Do not include any explanations or apologies in your responses.
        """

    def query_database(self, neo4j_query, params={}):
        with self.driver.session() as session:
            result = session.run(neo4j_query, params)
            output = [r.values() for r in result]
            output.insert(0, result.keys())
            return output

    def construct_cypher(self, question, history=None):
        messages = [
            {"role": "system", "content": self.get_system_message()},
            {"role": "user", "content": question},
        ]
        # Used for Cypher healing flows
        if history:
            messages.extend(history)

        completions = openai.ChatCompletion.create(
            model="gpt-4",
            temperature=0.0,
            max_tokens=150,  # Set to a lower value for concise responses
            messages=messages,
            request_timeout=60  # Set timeout period (in seconds)
        )
        return completions.choices[0].message.content


    def run(self, question, history=None, retry=True):
        # Construct Cypher statement
        cypher = self.construct_cypher(question, history)
        print(cypher)
        try:
            return self.query_database(cypher)
        # Self-healing flow
        except CypherSyntaxError as e:
            # If out of retries
            if not retry:
                return "Invalid Cypher syntax"
            # Self-healing Cypher flow by providing specific error to GPT-4
            print("Retrying")
            return self.run(
                question,
                [
                    {"role": "assistant", "content": cypher},
                    {
                        "role": "user",
                        "content": f"""This query returns an error: {str(e)} 
                        Give me a improved query that works without any explanations or apologies""",
                    },
                ],
                retry=False
            )

if __name__ == "__main__":
    # Load credentials from environment variables

    print(openai_key)

    gds_db = Neo4jGPTQuery(
        url=f"bolt://{server_url}:7687",
        openai_api_key=openai_key,
    )

    # Prompt for user input
    user_question = input("Please enter your question for the Neo4j database: ")
    gds_db.run(user_question)
