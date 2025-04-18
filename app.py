from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from datetime import datetime
from sqlalchemy import func
import json
app = Flask(__name__)
CORS(app, resources={
    r"/*": {
        "origins": "*",
        "methods": ["GET", "POST","OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization","ngrok-skip-browser-warning"],
        "expose_headers": ["X-Custom-Header"]
    }
})

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
            "country": item.country
        } for item in results])
    
    except Exception as e:
        app.logger.error(f"Database error: {str(e)}")
        return jsonify({"error": "Database operation failed"}), 500

@app.route('/word_cloud', methods=['GET'])
def word_cloud():
    try:
        results = YT.query.with_entities(
            YT.country,
            YT.category,
            YT.tags
        ).filter(YT.country == "CA").limit(1000).all()

        results += YT.query.with_entities(
            YT.country,
            YT.category,
            YT.tags
        ).filter(YT.country == "US").limit(1000).all()

        formatted_data = [
            {
                "country": item.country or "Unknown",
                "category": item.category or "Uncategorized",
                "tags": "|".join(
                    [tag.strip('"') for tag in item.tags.split('|')]
                ) if item.tags else ""
            }
            for item in results
        ]

        return jsonify(formatted_data)

    except Exception as e:
        app.logger.error(f"Word cloud error: {str(e)}")
        return jsonify({
            "error": "Failed to generate word cloud data",
            "details": str(e)
        }), 500



@app.route('/bar_chart', methods=['GET'])
def bar_chart():
    try:
        # Get query parameters from frontend
        country = request.args.get('country')
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')

        # Base query
        query = YT.query.with_entities(
            YT.category,
            func.sum(YT.likes).label('total_likes'),
            func.sum(YT.views).label('total_views'),
            func.count(YT.ID).label('video_count')
        )
        print(country)
        # Apply filters
        if country and country != "ALL":
            query = query.filter(YT.country == country)
        
        if start_date and end_date:
            query = query.filter(YT.timestamp.between(start_date, end_date))

        # Group by category and filter only the 7 main categories
        results = query.group_by(YT.category).all()

        # Format response
        formatted_data = [{
            "category": row.category or "Uncategorized",
            "likes": int(row.total_likes) or 0,
            "views": int(row.total_views) or 0,
            "videos": int(row.video_count) or 0
        } for row in results]

        return jsonify(formatted_data)

    except Exception as e:
        app.logger.error(f"Category stats error: {str(e)}")
        return jsonify({
            "error": "Failed to fetch category statistics",
            "details": str(e)
        }), 500

@app.route('/radar_chart', methods=['GET'])
def radar_chart():
    try:
        # Get query parameters
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')
        # Get and parse categories
        categories_param = request.args.get('categories', '[]')
        try:
            categories = json.loads(categories_param)
        except json.JSONDecodeError:
            categories = []
        # Base query
        query = db.session.query(
            YT.category,
            func.sum(YT.likes).label('likes'),
            func.sum(YT.views).label('views'),
            func.sum(YT.comments).label('comments'),
            func.sum(YT.dislikes).label('dislikes'),
            func.avg(YT.duration).label('avg_duration')
        )

        if start_date and end_date:
            query = query.filter(YT.timestamp.between(start_date, end_date))
       

        # Apply category filter
        if categories:
            query = query.filter(YT.category.in_(categories))
        print(categories, "aryan")
        # Group and execute
        results = query.group_by(YT.category).all()

        # Format response
        formatted_data = [{
            "category": row.category,
            "likes": int(row.likes) if row.likes else 0,
            "views": int(row.views) if row.views else 0,
            "comments": int(row.comments) if row.comments else 0,
            "dislikes": int(row.dislikes) if row.dislikes else 0,
            "avg_duration": round(float(row.avg_duration), 2) if row.avg_duration else 0.00
        } for row in results]

        return jsonify(formatted_data)

    except Exception as e:
        app.logger.error(f"Radar chart error: {str(e)}")
        return jsonify({
            "error": "Failed to generate radar chart data",
            "details": str(e)
        }), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0',port=8000, debug=True)
