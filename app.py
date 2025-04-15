from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from datetime import datetime

app = Flask(__name__)
CORS(app)

app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:aryan@localhost:5432/youtube_stats'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class YT(db.Model):
    __tablename__ = 'yt'
    
    ID = db.Column('ID', db.Text, primary_key=True)
    title = db.Column('title', db.Text)
    category = db.Column('category', db.Text)
    views = db.Column('#views', db.Integer)
    comments = db.Column('#comments', db.Integer)
    likes = db.Column('#likes', db.Integer)
    dislikes = db.Column('#dislikes', db.Integer)
    timestamp = db.Column('timestamp', db.Text)
    duration = db.Column('duration', db.Float)
    description = db.Column('description', db.Text)
    tags = db.Column('tags', db.Text)
    country = db.Column('country', db.Text)
    combined_text = db.Column('combined_text', db.Text)

def parse_timestamp(timestamp_str):
    """Convert string timestamp to ISO format"""
    try:
        for fmt in ('%Y-%m-%d','%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S'):
            try:
                dt = datetime.strptime(timestamp_str, fmt)
                return dt.isoformat()
            except ValueError:
                continue
        return timestamp_str  # Return raw string if parsing fails
    except Exception:
        return None

@app.route('/test', methods=['GET'])
def test():
    try:
        results = YT.query.limit(10).all()
        return jsonify([{
            "ID": item.ID,
            "title": item.title,
            "category": item.category,
            "views": item.views,
            "comments": item.comments,
            "likes": item.likes,
            "dislikes": item.dislikes,
            "timestamp": parse_timestamp(item.timestamp),
            "duration": item.duration,
            "description": item.description,
            "tags": item.tags,
            "country": item.country,
            "combined_text": item.combined_text
        } for item in results])
    
    except Exception as e:
        app.logger.error(f"Database error: {str(e)}")
        return jsonify({"error": "Database operation failed"}), 500

if __name__ == '__main__':
    app.run(port=5000, debug=True)