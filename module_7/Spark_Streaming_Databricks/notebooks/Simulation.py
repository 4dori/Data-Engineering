# Databricks notebook source
from pydantic import BaseSettings

class Settings(BaseSettings): 
    AZURE_ACC: str = "" 
    AZURE_ACC_KEY: str = ""
    
    class Config: 
        env_file = ".env" 
        case_sensitive = True
    
settings = Settings()

# COMMAND ----------

import time

def first_level_folders(path: str) -> list:
    children = [x for x in dbutils.fs.ls(path) if x.isDir()]
    if not children:
        return [path]
    
    rez = []
    for child_path in children:
        rez += first_level_folders(child_path.path)
    return rez

def rename_folders(src: str, dst: str, folders_list: list) -> list:
    return [path.replace(src, dst, 1) for path in folders_list]

def simulate_stream(src: str, dst: str, pause: float = 120.0) -> None:
    src_folders = first_level_folders(src)
    dst_folders = rename_folders(src, dst, src_folders)
    for src_f, dst_f in zip(src_folders, dst_folders):
        dbutils.fs.cp(src_f, dst_f, True)
        time.sleep(pause)


src_path = "dbfs:/mnt/input"
dst_path = "dbfs:/mnt/output"


url =  f"wasbs://data2@{settings.AZURE_ACC}.blob.core.windows.net/source/hotel-weather"
config = f"fs.azure.account.key.{settings.AZURE_ACC}.blob.core.windows.net"


dbutils.fs.mount(source = f"wasbs://data2@{settings.AZURE_ACC}.blob.core.windows.net/source/hotel-weather",  mount_point = src_path[5:], extra_configs = {config: settings.AZURE_ACC_KEY})
simulate_stream(src_path, dst_path, 120)



