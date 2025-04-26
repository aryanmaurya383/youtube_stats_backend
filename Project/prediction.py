import numpy as np
import pandas as pd
import lightgbm as lgb
import pickle
import os
from gensim.models import KeyedVectors

# -------------------------------
# Load all required artifacts
# -------------------------------

# Load GloVe model
glove_model = KeyedVectors.load_word2vec_format('./Project/Model_dir/glove.6B.50d.word2vec.txt', binary=False)

# Load LabelEncoders
with open('./Project/Model_dir/le_country.pkl', 'rb') as f:
    le_country = pickle.load(f)

with open('./Project/Model_dir/le_category.pkl', 'rb') as f:
    le_category = pickle.load(f)

# Load trained LightGBM models
models = {}
target_cols = ['#views', '#comments', '#likes', '#dislikes']
for target in target_cols:
    model_path = f'./Project/Model_dir/models/lgb_{target}.txt'
    models[target] = lgb.Booster(model_file=model_path)

# -------------------------------
# Helper functions
# -------------------------------

# Clean tags (replace '|' with space, limit to 512 characters)
def clean_tags(tags):
    if pd.isna(tags):
        return ''
    return str(tags).replace('|', ' ')[:512]

# Get average embedding for tags
def get_avg_embedding(tags, model=glove_model, dim=50):
    words = tags.split()
    embeddings = [model[word] for word in words if word in model]
    return np.mean(embeddings, axis=0) if embeddings else np.zeros(dim)

# -------------------------------
# Main prediction function
# -------------------------------

def predict_from_input(input_data):
    """
    input_data: dict with keys - 'tags', 'duration', 'country', 'category'
    Example:
    {
        'tags': "funny|cat|cute",
        'duration': 300,
        'country': "US",
        'category': "Music"
    }
    """
    # print("Input data:", input_data)
    # Prepare input DataFrame
    df_input = pd.DataFrame([input_data])

    if df_input['category'].iloc[0] not in ['People and Lifestyle', 'Music', 'Films', 'Travel and Vlogs','Science and Technology', 'Gaming and Sports', 'Current Affairs']:
        
        print("Enter valid category")
        
        return {}
    # print( df_input['category'].iloc[0])
    # print( df_input['country'].iloc[0])
    # print( df_input['tags'].iloc[0])
    # print(df_input['duration'].iloc[0])
    # Clean and process inputs
    df_input['clean_tags'] = df_input['tags'].apply(clean_tags)
    df_input['log_duration'] = np.log1p(df_input['duration'].clip(lower=0))
    df_input['country_encoded'] = le_country.transform(df_input['country'])
    df_input['category_encoded'] = le_category.transform(df_input['category'])

    # Get tag embeddings
    tag_embeds = df_input['clean_tags'].apply(get_avg_embedding)
    tag_embeds = np.vstack(tag_embeds.values)

    # Final feature matrix
    X_processed = np.hstack([
        df_input[['log_duration', 'country_encoded', 'category_encoded']].values,
        tag_embeds
    ])

    # Predict for each target
    predictions = {}
    for target in target_cols:
        pred_log = models[target].predict(X_processed)[0]
        pred = np.expm1(pred_log)  # Reverse log1p
        predictions[target] = pred

    return predictions

# -------------------------------
# Example usage
# -------------------------------

if __name__ == "__main__":
    sample_input = {
        'tags': "cat|cute|funny",
        'duration': 300,
        'country': "US",
        'category': "Music"
    }

    preds = predict_from_input(sample_input)
    print("Predictions (original scale):")
    for target, pred in preds.items():
        print(f"{target}: {pred:.2f}")



