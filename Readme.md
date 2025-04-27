# YouTube Trending Videos Analytics Backend 🖥️

This backend system was developed as part of the CS661 Big Data Visual Analytics course project under Professor Soumya Dutta. It serves as the data processing and API layer for analyzing YouTube trending video statistics.

## System Overview

A Flask-based backend service that provides:

- REST API endpoints for data visualization
- Machine learning predictions for video performance
- Connection to PostgreSQL database
- Data processing pipelines for big data analytics

## 🔗 Resources

- **Dataset & Models**: [Hugging Face Repository](https://huggingface.co/datasets/aryanmaurya383/cs661-big-data-project-dataset)
  - Dataset ZIP: `youtube_trending_data.zip`
  - Model Files: `glove.6B.50d.word2vec`, `le_country.pkl`, `le_category.pkl`
- **Live API**: `https://171d-202-3-77-209.ngrok-free.app/`

## 🛠️ Installation & Setup

### Prerequisites

- Python 3.8+
- PostgreSQL 12+

### Setup Instructions

1.  Download all the files from [Hugging Face Repository](https://huggingface.co/datasets/aryanmaurya383/cs661-big-data-project-dataset)

### How to Run

1. Install PostgreSQL (PSQL).
2. Download all the files from the [Hugging Face Repository](https://huggingface.co/datasets/aryanmaurya383/cs661-big-data-project-dataset), including the ZIP and model files.
3. Extract the ZIP file to get a CSV file. Place it in the `CreatePSQL_db` folder.
4. Open the `createDB.py` file inside `CreatePSQL_db` folder and update the username and password as per your PostgreSQL configuration.
5. Run the `createDB.py` file. After completion, a PostgreSQL database named `youtube_stats` will be ready.
6. Copy the three model files [`glove.6B.50d.word2vec`, `le_country.pkl`, `le_category.pkl`] into the `Project/Model_dir` folder.
7. Run `pip install -r requirements.txt` to install dependencies.
8. Open `app.py` and update the `USERNAME` and `PASSWORD` variables as per your PostgreSQL configuration.
9. Start the `app.py` file to run the backend, which will now be connected to the PostgreSQL database.
10. To test the setup, send a GET request to the `/test` endpoint.
