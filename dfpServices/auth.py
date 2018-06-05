import os
import yaml
from googleads import dfp
from flask import json

dfp_client = None


def getDFPClient():
    
   
    global dfp_client
    if(dfp_client == None):
        dfp_client = dfp.DfpClient.LoadFromStorage()
        
    return dfp_client


    