import findspark
findspark.init()
import logging
import time, sys, cherrypy, os
from paste.translogger import TransLogger
from app import create_app
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

def init_spark_context():
    
    enginePath = os.path.join(os.getcwd(), "engine.py")
    appPath = os.path.join(os.getcwd(), "app.py")
    # load spark context
    sc = SparkSession \
            .builder \
            .appName("FP Recommendation REST API") \
            .getOrCreate()
    
    # IMPORTANT: pass aditional Python modules to each worker
    sc.sparkContext.addPyFile(enginePath)
    sc.sparkContext.addPyFile(appPath)

    return sc
 
 
def run_server(app):
 
    # Enable WSGI access logging via Paste
    app_logged = TransLogger(app)
 
    # Mount the WSGI callable object (app) on the root directory
    cherrypy.tree.graft(app_logged, '/')
 
    # Set the configuration of the web server
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 5432,
        'server.socket_host': '0.0.0.0'
    })
 
    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()
 
 
if __name__ == "__main__":
    # Init spark context and load libraries
    sc = init_spark_context()
    dataset_path = os.path.join(os.getcwd(), "dataset.csv")
    app = create_app(sc, dataset_path)
 
    # start web server
    run_server(app)

