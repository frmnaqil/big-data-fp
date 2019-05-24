from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
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
        (self.training, self.test) = self.join2.randomSplit([0.7,0.3])

        # Build the recommendation model using ALS on the training data
        # Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics

        als = ALS(maxIter=5,
                 regParam=0.01,
                userCol="newUserId",
                itemCol="newItemId",
                ratingCol="rating",
                coldStartStrategy="drop")

        self.model = als.fit(self.training)

        logger.info("ALS model built!")

    def __evaluate(self):
        predictions = self.model.transform(self.test)
        evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")
        rmse = evaluator.evaluate(predictions)
        logger.info("Root-Mean-Square Error = %s" % rmse)

    def __join(self):

        logger.info("Start Join")

        self.ratings.createOrReplaceTempView("ratings")

        logger.info("Create View Success")

        ratings_userId_distinct = self.sc.sql("SELECT userId, ROW_NUMBER() OVER(ORDER BY userId) as newUserId FROM (SELECT DISTINCT userId FROM ratings)")

        ratings_itemId_distinct = self.sc.sql("SELECT itemId, ROW_NUMBER() OVER(ORDER BY itemId) as newItemId FROM (SELECT DISTINCT itemId FROM ratings)")

        join1 = self.ratings\
            .join(ratings_userId_distinct, ratings_userId_distinct.userId == self.ratings.userId, 'inner')\
            .select(self.ratings.itemId, self.ratings.rating, self.ratings.userId, ratings_userId_distinct.newUserId)
        
        self.join2 = join1\
            .join(ratings_itemId_distinct, ratings_itemId_distinct.itemId == join1.itemId, 'inner')\
            .select(join1.newUserId, ratings_itemId_distinct.newItemId, join1.rating)

        logger.info("Join Success!")

    def recommended_for_user(self, userId, items, model):

        self.join2.createOrReplaceTempView("joined")

        logger.info("recommending")

        user = self.sc.sql("SELECT newUserId FROM joined WHERE newUserId = {}".format(userId))
        userSubsetRecs = self.model.recommendForUserSubset(user, items)

        userSubsetRecs = userSubsetRecs.toPandas()
        userSubsetRecs = userSubsetRecs.to_json()

        return userSubsetRecs

    def __init__(self, sc, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """

        logger.info("Starting up the Recommendation Engine: ")

        # Load Dataset
        logger.info("Loading Dataset")

        self.sc = sc

        lines = sc.read.text(dataset_path).rdd
        parts = lines.map(lambda row: row.value.split(","))
        ratingsRDD = parts.map(lambda p: Row(userId=p[0], itemId=p[1], rating=float(p[2]), timestamp=int(p[3])))
        self.ratings = sc.createDataFrame(ratingsRDD)

        self.__join()
        self.__train_model()
        # self.__evaluate()


