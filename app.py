from flask import Flask, jsonify
import psycopg2
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Enable CORS for cross-origin requests

DB_CONFIG = {
    "host": "localhost",
    "database": "youtube_stats",
    "user": "postgres",
    "password": "aryan",
    "port": "5432"  # Default PostgreSQL port
}

@app.route('/test', methods=['GET'])
def get_data():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Execute query
        cursor.execute("SELECT * FROM yt LIMIT 10")
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Format results
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
            
        # Close connection
        cursor.close()
        conn.close()
        
        return jsonify(results)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(port=5000, debug=True)