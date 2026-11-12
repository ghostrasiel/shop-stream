"""Spark schema for the JSON payload.

Keep this aligned with generator/schema/ecommerce_event.json. M1 parses with
this fixed struct instead of asking Schema Registry for the writer schema.
The schema id from the wire prefix is still carried on the row.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

LINE_ITEM = StructType(
    [
        StructField("sku", StringType()),
        StructField("name", StringType()),
        StructField("category", StringType()),
        StructField("qty", IntegerType()),
        StructField("unit_price", DoubleType()),
    ]
)

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_time", StringType()),
        StructField("user_id", StringType()),
        StructField("session_id", StringType()),
        StructField(
            "payload",
            StructType(
                [
                    StructField(
                        "device",
                        StructType(
                            [
                                StructField("type", StringType()),
                                StructField("os", StringType()),
                                StructField("app_version", StringType()),
                            ]
                        ),
                    ),
                    StructField(
                        "utm",
                        StructType(
                            [
                                StructField("source", StringType()),
                                StructField("medium", StringType()),
                                StructField("campaign", StringType()),
                            ]
                        ),
                    ),
                    StructField(
                        "page",
                        StructType(
                            [
                                StructField("url", StringType()),
                                StructField("referrer", StringType()),
                            ]
                        ),
                    ),
                    StructField("product", LINE_ITEM),
                    StructField(
                        "order",
                        StructType(
                            [
                                StructField("order_id", StringType()),
                                StructField("currency", StringType()),
                                StructField("items", ArrayType(LINE_ITEM)),
                                StructField("total_amount", DoubleType()),
                            ]
                        ),
                    ),
                    StructField(
                        "payment",
                        StructType(
                            [
                                StructField("payment_id", StringType()),
                                StructField("order_id", StringType()),
                                StructField("amount", DoubleType()),
                                StructField("method", StringType()),
                                StructField("status", StringType()),
                            ]
                        ),
                    ),
                ]
            ),
        ),
    ]
)

FRAME_SCHEMA = StructType(
    [
        StructField("schema_id", IntegerType()),
        StructField("json_payload", StringType()),
    ]
)
