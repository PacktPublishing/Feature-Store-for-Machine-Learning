from feast import Entity, ValueType, FeatureView, Feature, RedshiftSource
from datetime import timedelta

# Customer ID entity definition.
customer = Entity(
    name='customer',
    value_type=ValueType.STRING,
    join_key='CustomerID',
    description="Id of the customer",
    tags=["customer_segmentation", "LTV"]
)

# Redshift batch source
rfm_features_source = RedshiftSource(
    query="SELECT * FROM spectrum.customer_rfm_features",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created_timestamp",
)

# FeatureView definition for RFM features.
rfm_features_features = FeatureView(
    name="customer_rfm_features",

    entities=["customer"],
    ttl=timedelta(days=3650),
    features=[
        Feature(name="Recency", dtype=ValueType.INT32),
        Feature(name="Frequency", dtype=ValueType.INT32),
        Feature(name="MonetaryValue", dtype=ValueType.INT64),
        Feature(name="R", dtype=ValueType.INT32),
        Feature(name="F", dtype=ValueType.INT32),
        Feature(name="M", dtype=ValueType.INT32),
        Feature(name="RFMScore", dtype=ValueType.INT32),
        Feature(name="Revenue_6m", dtype=ValueType.INT32),
        Feature(name="LTVCluster", dtype=ValueType.INT32),
        Feature(name="Segment_High-Value", dtype=ValueType.INT32),
        Feature(name="Segment_Low-Value", dtype=ValueType.INT32),
        Feature(name="Segment_Mid-Value", dtype=ValueType.INT32),
    ],
    batch_source=rfm_features_source,
)
