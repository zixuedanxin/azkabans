import os
from  azkaban.properties import  Properties
input = os.getenv("JOB_PROP_FILE")
prop=Properties(input)
prop.get_properties()
print(prop.properties.keys())
# fs=open(input,'r')
# with fs:
#     strs=fs.read()
# print(strs)
import json
output = os.getenv("JOB_OUTPUT_PROP_FILE")
with open(output,"a+") as fout:
    fout.write(json.dumps({'key':'hua'}))