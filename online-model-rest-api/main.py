import os.path

from fastapi import FastAPI
from feast import FeatureStore
import pandas as pd
import boto3
import joblib

app = FastAPI()


@app.get("/ping")
def ping():
    return {"ping": "ok"}


store = FeatureStore(repo_path=os.path.join(os.getcwd(), "customer_segmentation"))
model_name = "customer_segment-v0.0"
s3 = boto3.client('s3')
s3.download_file(
    "feast-demo-mar-2022",
    f"model-repo/{model_name}",
    model_name)

loaded_model = joblib.load('customer_segment-v0.0')


@app.post("/invocations")
def inference(customers: dict):
    required_features = [
        "customer_rfm_features:recency",
        "customer_rfm_features:monetaryvalue",
        "customer_rfm_features:r",
        "customer_rfm_features:m",
        "customer_rfm_features:rfmscore",
        "customer_rfm_features:segmenthighvalue",
        "customer_rfm_features:segmentlowvalue",
        "customer_rfm_features:segmentmidvalue"
    ]
    entity_rows = [{"customer": cust_id} for cust_id in customers["customer_list"]]
    feature_vector = store.get_online_features(
        features=required_features,
        entity_rows=entity_rows,
    ).to_dict()
    features_in_order = ['recency', 'monetaryvalue', 'r', 'm', 'rfmscore',
                         'segmenthighvalue', 'segmentlowvalue', 'segmentmidvalue']
    df = pd.DataFrame(feature_vector)
    features = df.drop(['customerid'], axis=1)
    features = features.dropna()
    features = features[features_in_order]
    prediction = loaded_model.predict(features)
    return {"predictions": prediction.tolist()}

