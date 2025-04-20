from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from datetime import datetime
from sqlalchemy import func, text
import json
import calendar
from datetime import date
from collections import defaultdict
import time

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

def add_dates_filter_to_query(query, start_mon, end_mon):
    if start_mon and end_mon:
        # Parse YYY-MM input to date objects
        start_year, start_month = map(int, start_mon.split('-'))
        end_year, end_month = map(int, end_mon.split('-'))
        
        # Create first day of start month
        start_date = date(start_year, start_month, 1)
        
        # Create last day of end month
        last_day = calendar.monthrange(end_year, end_month)[1]
        end_date = date(end_year, end_month, last_day)
        
        return query.filter(YT.timestamp.between(start_date, end_date))
    return query



class YT(db.Model):
    __tablename__ = 'yt'
    
    ID = db.Column('ID', db.Text, primary_key=True)
    title = db.Column('title', db.Text)
    category = db.Column('category', db.Text)
    views = db.Column('#views', db.Integer)
    comments = db.Column('#comments', db.Integer)
    likes = db.Column('#likes', db.Integer)
    dislikes = db.Column('#dislikes', db.Integer)
    timestamp = db.Column('timestamp', db.Date)  
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
            "timestamp": item.timestamp.isoformat() if item.timestamp else None,
            "duration": item.duration,
            "description": item.description,
            "tags": item.tags,
            "country": item.country
        } for item in results])
    
    except Exception as e:
        app.logger.error(f"Database error: {str(e)}")
        return jsonify({"error": "Database operation failed"}), 500

MAX_WORDS = 150
MIN_OCCURRENCE = 3
@app.route('/word_cloud', methods=['GET'])
def word_cloud():
    try:
        # Get query parameters
        country = request.args.get('country')
        category = request.args.get('category')
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')

        # Build the SQL query with proper tag cleaning and filtering
        sql = """
        SELECT lower(trim(both '"' from tag)) as cleaned_tag, COUNT(*) as count
        FROM (
            SELECT unnest(string_to_array(tags, '|')) as tag
            FROM yt
            WHERE tags IS NOT NULL AND tags != ''
        """
        params = {
            'min_occurrence': MIN_OCCURRENCE,
            'max_words': MAX_WORDS
        }

        # Apply filters
        if country:
            sql += " AND country = :country"
            params['country'] = country
        if category:
            sql += " AND category = :category"
            params['category'] = category
        
        if start_date and end_date:
            # Convert to date objects
            start_year, start_month = map(int, start_date.split('-'))
            end_year, end_month = map(int, end_date.split('-'))
            
            start_full = date(start_year, start_month, 1)
            last_day = calendar.monthrange(end_year, end_month)[1]
            end_full = date(end_year, end_month, last_day)
            
            sql += " AND timestamp BETWEEN :start_date AND :end_date"
            params['start_date'] = start_full
            params['end_date'] = end_full

        sql += """
        ) sub
        WHERE tag != ''  -- Exclude empty tags
        GROUP BY lower(trim(both '"' from tag))
        HAVING COUNT(*) >= :min_occurrence
        ORDER BY count DESC
        LIMIT :max_words
        """

        # Execute query
        results = db.session.execute(text(sql), params).fetchall()

        # Format response
        word_data = [{"text": row.cleaned_tag, "value": int(row.count)} 
                     for row in results if row.cleaned_tag]

        return jsonify(word_data)

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
        
        query=add_dates_filter_to_query(query, start_date, end_date)

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

        query=add_dates_filter_to_query(query, start_date, end_date)     

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

@app.route('/world_map', methods=['GET'])
def world_map():
    try:
        # Get parameters from request
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')
        metric = request.args.get('metric', 'likes').lower()

        # Map metric to database column
        metric_columns = {
            'likes': YT.likes,
            'dislikes': YT.dislikes,
            'views': YT.views,
            'comments': YT.comments
        }

        if metric not in metric_columns:
            return jsonify({"error": "Invalid metric"}), 400

        metric_col = metric_columns[metric]

        # Base query with filters
        query = db.session.query(
            YT.country,
            YT.category,
            func.sum(metric_col).label('total')
        )

        query=add_dates_filter_to_query(query, start_date, end_date)

        # Execute query and group results
        results = query.group_by(YT.country, YT.category).all()

        # Process results into country-based format
        country_map = {}
        categories = [
            'Current Affairs', 'Films', 'Gaming and Sports',
            'Music', 'People and Lifestyle', 
            'Science and Technology', 'Travel and Vlogs'
        ]

        for country, category, total in results:
            if country not in country_map:
                # Initialize with all categories at 0
                country_map[country] = {'country': country}
                for cat in categories:
                    country_map[country][cat] = 0.0
            
            if category in categories:
                country_map[country][category] = float(total) if total else 0.0

        # Convert to list and fill missing countries
        formatted_data = list(country_map.values())

        return jsonify(formatted_data)

    except Exception as e:
        app.logger.error(f"World map error: {str(e)}")
        return jsonify({
            "error": "Failed to generate world map data",
            "details": str(e)
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=8000, debug=True)
