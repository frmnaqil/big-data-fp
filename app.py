from flask import Blueprint
main = Blueprint('main', __name__)
 
import json
from engine import RecommendationEngine
 
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
from flask import Flask, request
@main.route("/user/<int:ids>/model/<int:model>/<int:value>", methods=["GET"])
def userRecommend(ids,value,model):
    logger.info("TOP %s item recommendation for user %s" % (value,ids))
    result = recommendation_engine.recommended_for_user(ids, value, model)
    return json.dumps(result)


def create_app(spark_context, dataset_path):
    global recommendation_engine

    recommendation_engine = RecommendationEngine(spark_context, dataset_path)    
    
    app = Flask(__name__)
    app.register_blueprint(main)
    return app 
