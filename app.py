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
import re
from Project.prediction import predict_from_input as predict_stats_from_input

app = Flask(__name__)
CORS(app, resources={
    r"/*": {
        "origins": "*",
        "methods": ["GET", "POST","OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization","ngrok-skip-browser-warning"],
        "expose_headers": ["X-Custom-Header"]
    }
})

USERNAME="postgres"
PASSWORD="aryan"
app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{USERNAME}:{PASSWORD}@localhost:5432/youtube_stats'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


def convert_to_full_dates(start_date, end_date):
    start_year, start_month = map(int, start_date.split('-'))
    end_year, end_month = map(int, end_date.split('-'))
    
    start_full = date(start_year, start_month, 1)
    last_day = calendar.monthrange(end_year, end_month)[1]
    end_full = date(end_year, end_month, last_day)
    return start_full, end_full

def add_dates_filter_to_query(query, start_mon, end_mon):
    if start_mon and end_mon:
        # Parse YYY-MM input to date objects
        start_date,end_date=convert_to_full_dates(start_mon, end_mon)
        
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
            WHERE tags IS NOT NULL AND tags != '' AND tags != '[none]'
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
            start_full, end_full = convert_to_full_dates(start_date, end_date)
            
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
            func.sum(YT.comments).label('total_comments'),
            func.sum(YT.dislikes).label('total_dislikes'),
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
            "likes": int(row.total_likes/1000000) or 0,
            "views": int(row.total_views/1000000) or 0,
            "videos": int(row.video_count) or 0,
            "comments": int(row.total_comments/1000000) or 0,
            "dislikes": int(row.total_dislikes/1000000) or 0
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


@app.route('/corr_mat', methods=['GET'])
def corr_mat():
    try:
        start_mon = request.args.get('startDate')
        end_mon = request.args.get('endDate')
        country = request.args.get('country', 'US')

        start_date=None
        end_date=None
        if start_mon and end_mon:
            start_date,end_date=convert_to_full_dates(start_mon, end_mon)

        sql = '''
        SELECT 
            COALESCE(category, 'ALL') as category,
            COALESCE(CORR("#likes", "#views"), 0) as likes_views,
            COALESCE(CORR("#likes", "#dislikes"), 0) as likes_dislikes,
            COALESCE(CORR("#likes", "#comments"), 0) as likes_comments,
            COALESCE(CORR("#likes", "duration"), 0) as likes_duration,
            COALESCE(CORR("#views", "#dislikes"), 0) as views_dislikes,
            COALESCE(CORR("#views", "#comments"), 0) as views_comments,
            COALESCE(CORR("#views", "duration"), 0) as views_duration,
            COALESCE(CORR("#dislikes", "#comments"), 0) as dislikes_comments,
            COALESCE(CORR("#dislikes", "duration"), 0) as dislikes_duration,
            COALESCE(CORR("#comments", "duration"), 0) as comments_duration
        FROM yt
        WHERE (:start_date IS NULL OR timestamp >= :start_date) AND
            (:end_date IS NULL OR timestamp <= :end_date)  AND
            (country = :country OR :country = 'ALL')
        GROUP BY GROUPING SETS ((category), ())
        '''

        results = db.session.execute(text(sql), {
            'start_date': start_date,
            'end_date': end_date,
            'country': country
        }).fetchall()

        formatted_data = [
            {
                "category": row.category,
                "likes_views": float(row.likes_views),
                "likes_dislikes": float(row.likes_dislikes),
                "likes_comments": float(row.likes_comments),
                "likes_duration": float(row.likes_duration),
                "views_dislikes": float(row.views_dislikes),
                "views_comments": float(row.views_comments),
                "views_duration": float(row.views_duration),
                "dislikes_comments": float(row.dislikes_comments),
                "dislikes_duration": float(row.dislikes_duration),
                "comments_duration": float(row.comments_duration)
            }
            for row in results
        ]

        return jsonify(formatted_data)
    except Exception as e:
        app.logger.error(f"Correlation matrix error: {str(e)}")
        return jsonify({
            "error": "Failed to generate correlation matrix",
            "details": str(e)
        }), 500


@app.route('/month_cat', methods=['GET'])
def monthly_category_metrics():
    try:
        # Get parameters
        start_mon = request.args.get('startDate')
        end_mon = request.args.get('endDate')
        metric = request.args.get('metric', 'likes').lower()
        country = request.args.get('country', 'ALL').upper()  # New parameter
        
        # Validate metric
        valid_metrics = {'likes', 'views', 'comments', 'dislikes'}
        if metric not in valid_metrics:
            return jsonify({"error": "Invalid metric"}), 400

        # Convert to full dates
        start_date, end_date = convert_to_full_dates(start_mon, end_mon)
        
        # Build query
        base_sql = """
        SELECT 
            category,
            DATE_TRUNC('month', timestamp) AS month,
            SUM("#{metric}") AS total,
            COUNT(*) AS video_count
        FROM yt
        WHERE timestamp BETWEEN :start_date AND :end_date
        """

        # Add country condition
        if country != 'ALL':
            base_sql += " AND country = :country "

        base_sql += """
        GROUP BY category, DATE_TRUNC('month', timestamp)
        ORDER BY DATE_TRUNC('month', timestamp), category
        """

        sql = text(base_sql.format(metric=metric))

        # Prepare parameters
        params = {
            'start_date': start_date,
            'end_date': end_date
        }
        if country != 'ALL':
            params['country'] = country

        # Execute query
        results = db.session.execute(sql, params).fetchall()

        # Create date range (original code unchanged)
        dates = []
        current = start_date.replace(day=1)
        end = end_date.replace(day=1)
        while current <= end:
            dates.append(current.strftime("%Y-%m"))
            if current.month == 12:
                current = current.replace(year=current.year+1, month=1)
            else:
                current = current.replace(month=current.month+1)
        
        # Format response (original code unchanged)
        response = {}
        for row in results:
            category = row.category
            month_str = row.month.strftime("%Y-%m")
            
            if category not in response:
                response[category] = {
                    'category': category,
                    'data': {date: 0 for date in dates}
                }
            
            response[category]['data'][month_str] =  round(row.total / row.video_count, 2) if row.video_count > 0 else 0

        # Convert to list and fill missing months
        formatted_data = []
        for cat in response.values():
            formatted_data.append({
                'category': cat['category'],
                'months': [{'month': date, 'total': cat['data'][date]} for date in dates]
            })

        return jsonify(formatted_data)

    except Exception as e:
        app.logger.error(f"Monthly category metrics error: {str(e)}")
        return jsonify({
            "error": "Failed to generate monthly category metrics",
            "details": str(e)
        }), 500

@app.route('/month_specific', methods=['GET'])
def month_specific():
    try:
        # Get parameters
        year_month = request.args.get('month')
        
        # Validate input format
        if not re.match(r'^\d{4}-\d{2}$', year_month):
            return jsonify({"error": "Invalid month format. Use YYYY-MM"}), 400
            
        year, month = map(int, year_month.split('-'))
        
        # Validate month range
        if month < 1 or month > 12:
            return jsonify({"error": "Invalid month. Must be 01-12"}), 400

        # Calculate month start/end dates
        first_day = date(year, month, 1)
        last_day = date(year, month, calendar.monthrange(year, month)[1])

        # Query database
        sql = text("""
        SELECT 
            COALESCE(SUM("#views"), 0) AS total_views,
            COALESCE(SUM("#likes"), 0) AS total_likes,
            COALESCE(SUM("#comments"), 0) AS total_comments
        FROM yt
        WHERE timestamp BETWEEN :start_date AND :end_date
        """)

        result = db.session.execute(sql, {
            'start_date': first_day,
            'end_date': last_day
        }).fetchone()

        # Format response
        response = {
            "month": year_month,
            "totals": {
                "views": int(result.total_views),
                "likes": int(result.total_likes),
                "comments": int(result.total_comments)
            }
        }

        return jsonify(response)

    except ValueError as ve:
        return jsonify({"error": f"Invalid date parameters: {str(ve)}"}), 400
    except Exception as e:
        app.logger.error(f"Month specific error: {str(e)}")
        return jsonify({
            "error": "Failed to retrieve monthly totals",
            "details": str(e)
        }), 500


@app.route('/predict', methods=['POST'])
def predict():
    try:
        # Get JSON data from request
        input_data = request.get_json()
        
        # Validate required fields
        required_fields = ['tags', 'duration', 'country', 'category']
        if not all(field in input_data for field in required_fields):
            return jsonify({"error": "Missing required field(s)"}), 400

        # Convert duration to numeric
        try:
            input_data['duration'] = float(input_data['duration'])
        except ValueError:
            return jsonify({"error": "Invalid duration format"}), 400

        # Make prediction
        predictions = predict_stats_from_input(input_data)
        
        # Handle invalid category case
        if not predictions:
            return jsonify({"error": "Invalid category specified"}), 400

        # Format response with rounded numbers
        formatted_response = {
            "predictions": {
                "#views": round(predictions.get('#views', 0)),
                "#likes": round(predictions.get('#likes', 0)),
                "#comments": round(predictions.get('#comments', 0)),
                "#dislikes": round(predictions.get('#dislikes', 0))
            },
            "status": "success"
        }

        return jsonify(formatted_response)

    except Exception as e:
        app.logger.error(f"Prediction error: {str(e)}")
        return jsonify({
            "error": "Prediction failed",
            "details": str(e),
            "status": "error"
        }), 500

@app.route('/temp', methods=['GET'])
def temp():
    try:
        # Get parameters (removed metric)
        start_mon = request.args.get('startDate', "2017-01")
        end_mon = request.args.get('endDate', "2021-12")
        
        # Convert to full dates
        start_date, end_date = convert_to_full_dates(start_mon, end_mon)
        
        # Modified SQL query (COUNT instead of SUM)
        sql = text("""
        SELECT 
            DATE_TRUNC('month', timestamp) AS month,
            COUNT(*) AS total
        FROM yt
        WHERE timestamp BETWEEN :start_date AND :end_date
        GROUP BY DATE_TRUNC('month', timestamp)
        ORDER BY DATE_TRUNC('month', timestamp)
        """)

        # Execute query
        results = db.session.execute(sql, {
            'start_date': start_date,
            'end_date': end_date
        }).fetchall()

        # Create date range (unchanged)
        dates = []
        current = start_date.replace(day=1)
        end = end_date.replace(day=1)
        while current <= end:
            dates.append(current.strftime("%Y-%m"))
            if current.month == 12:
                current = current.replace(year=current.year+1, month=1)
            else:
                current = current.replace(month=current.month+1)
        
        # Initialize response with all months
        monthly_data = {date: 0 for date in dates}
        
        # Fill with actual data
        for row in results:
            month_str = row.month.strftime("%Y-%m")
            monthly_data[month_str] = int(row.total)  # COUNT(*) always returns an integer

        # Format response
        formatted_data = [{'month': date, 'total': monthly_data[date]} 
                         for date in dates]

        return jsonify(formatted_data)

    except Exception as e:
        app.logger.error(f"Monthly counts error: {str(e)}")
        return jsonify({
            "error": "Failed to generate monthly counts",
            "details": str(e)
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=8000, debug=True)
