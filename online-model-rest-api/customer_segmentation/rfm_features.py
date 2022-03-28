from feast import Entity, ValueType, FeatureView, Feature, RedshiftSource
from datetime import timedelta

# Customer ID entity definition.
customer = Entity(
    name='customer',
    value_type=ValueType.STRING,
    join_key='customerid',
    description="Id of the customer"
)

# Redshift batch source
rfm_features_source = RedshiftSource(
    query="SELECT * FROM spectrum.customer_rfm_features",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created_timestamp",
)

rfm_features_features = FeatureView(
    name="customer_rfm_features",

    entities=["customer"],
    ttl=timedelta(days=3650),
    features=[
        Feature(name="recency", dtype=ValueType.INT32),
        Feature(name="frequency", dtype=ValueType.INT32),
        Feature(name="monetaryvalue", dtype=ValueType.DOUBLE),
        Feature(name="r", dtype=ValueType.INT32),
        Feature(name="f", dtype=ValueType.INT32),
        Feature(name="m", dtype=ValueType.INT32),
        Feature(name="rfmscore", dtype=ValueType.INT32),
        Feature(name="revenue6m", dtype=ValueType.DOUBLE),
        Feature(name="ltvcluster", dtype=ValueType.INT32),
        Feature(name="segmenthighvalue", dtype=ValueType.INT32),
        Feature(name="segmentlowvalue", dtype=ValueType.INT32),
        Feature(name="segmentmidvalue", dtype=ValueType.INT32),
    ],
    batch_source=rfm_features_source,
)
