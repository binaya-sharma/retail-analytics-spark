import yaml
from pyspark.sql import SparkSession

def load_config(path="conf/setting.yml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def build_spark(cfg):
    b = (SparkSession.builder
         .master(cfg["spark"]["master"])
         .appName(cfg["spark"]["appName"]))
    
    for k, v in cfg["spark"]["conf"].items():
        b = b.config(k, v)
    return b.enableHiveSupport().getOrCreate()