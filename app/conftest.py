import json
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from utils.logutils import LogUtils
from utils.apputils import AppUtils
from py4j.protocol import Py4JJavaError


class SparkInstanceBuilder:
    def __init__(self, environment=None, retries=2, retry_window=20, app_name=None):
        try:
            self.sparkEnv = environment
            self.retry = 0
            self.sparkNode = ""
            self.retry_window = retry_window
            self.sparkRetry = retries
            self.app_name = app_name
            self.logger = LogUtils.logger()

            if self.sparkEnv is None or self.sparkEnv == "" or self.app_name is None or self.app_name == "":
                raise Exception("Missing mandatory parameters, terminating the request to create a spark instance")

            self.sparkClusterMataInfo = AppUtils.convertToDictionary(open("../config/spark_instance_info.json", 'r').read())

            if self.sparkClusterMataInfo is None or self.sparkClusterMataInfo == "":
                self.logger.error("Process failed while fetching spark cluster details from zookeeper, terminating "
                                  "the request. Please, validate details under zookeeper config")
                raise Exception("Process failed while fetching spark cluster details from zookeeper, terminating the "
                                "request. Please, validate details under zookeeper config")
        except Exception as initiateErr:
            self.logger.error(
                f"Process to set initial config parameters has failed with error - {initiateErr}. Terminating the "
                f"process")

    def buildSpark(self, env=None, corePerExecutor=None, MaxCorePerProcess=None, MaxMemForDriver=None,
                   MaxMemPerExecutor=None, enableAuth="N", enableEventLogs="N", secret=None, clusterMode=None):
        try:
            conf = SparkConf()
            conf.setAppName(f"{self.app_name}-{env}")
            conf.setMaster(self.sparkNode)
            conf.set('spark.submit.deployMode', f'{clusterMode}')  # Corrected typo in property name

            for spark_config_properties in self.sparkClusterMataInfo['sparkSessionProperties']:
                if spark_config_properties == "spark.executor.cores":
                    conf.set("spark.executor.cores", corePerExecutor)
                elif spark_config_properties == "spark.cores.max":
                    conf.set("spark.cores.max", MaxCorePerProcess)
                elif spark_config_properties == "spark.driver.memory":
                    conf.set("spark.driver.memory", MaxMemForDriver)
                elif spark_config_properties == "spark.executor.memory":
                    conf.set("spark.executor.memory", MaxMemPerExecutor)
                else:
                    conf.set(f"{spark_config_properties}",
                             f"{self.sparkClusterMataInfo['sparkSessionProperties'][spark_config_properties]}")

            if enableAuth == "Y":
                conf.set('spark.authenticate', "true")
                conf.set('spark.authenticate.secret', secret)
            if enableEventLogs == "Y":
                conf.set('spark.eventLog.enabled', "true")
                conf.set('spark.eventLog.dir', self.sparkClusterMataInfo['appConfig']['EVENT_LOG_DIRECTORY'])

            self.logger.info(
                f"Spark Configuration Parameters are - {conf.toDebugString()}, initiating spark using same")
            spark = SparkSession.builder.config(conf=conf).getOrCreate().newSession()
            self.logger.info(
                f"Spark instance has been created successfully on node - {self.sparkNode} for app {self.app_name} "
                f"in {env} check on spark node - {self.sparkNode}.")
            return spark
        except Py4JJavaError as sparkInstanceErr:
            self.logger.error(
                f"Process to create spark instance has failed due to {sparkInstanceErr} for app {self.app_name} in "
                f"{env} check on spark node - {self.sparkNode}")
            return "Failure"
        except Exception as sparkInstanceGenericErr:
            self.logger.error(
                f"Process to create spark instance has failed due to {sparkInstanceGenericErr} for app {self.app_name} "
                f"in {env} check on spark node - {self.sparkNode}")
            return "Failure"
