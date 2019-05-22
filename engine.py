from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
import pandas as pd

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RecommendationEngine:
    """A item recommendation engine
    """

    def __train_model(self):
        """Train the ALS model with the current dataset
        """
        logger.info("Training the ALS model...")

        # Train, Test
        (training, test) = self.ratings.randomSplit([0.7,0.3])

        # Build the recommendation model using ALS on the training data
        # Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics

        als = ALS(maxIter=5, regParam=0.01, userCol="user_id", itemCol="item_id", ratingCol="rating", coldStartStrategy="drop")

        self.model = als.fit(training)

        logger.info("ALS model built!")

    def __init__(self, sc, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """

        logger.info("Starting up the Recommendation Engine: ")
