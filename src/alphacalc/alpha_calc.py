from datetime import datetime
import logging
from multiprocessing import Pool
import importlib

# Local 
from pathmgmt import pathmgmt as myPath

def compute_single_alpha(cfg):
    algo, kwargs = cfg
    alphaClass = getattr(importlib.import_module(f"alphas.{algo}"), algo)
    alphaObj = alphaClass(kwargs)
    alphaObj.compute()

def calc(cfg_dict):
    '''cfg_dict is a list: [(alpha_name, **kwargs), ...]'''
    # multiprocessing here
    with Pool() as pool:
        pool.map(compute_single_alpha, cfg_dict)
