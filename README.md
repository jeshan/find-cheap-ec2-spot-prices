# find-cheap-ec2-spot-prices
Helps you find cheap instances across all regions based on specified specs.

It's a python script that you can run as follows:
`python main.py $MAX_PRICE_USD_PER_HOUR $MIN_VCPU_COUNT $MIN_MEMORY_MIB`

e.g:

`python main.py 0.0199 2 8000`

will look for spot instances currently costing **at most** $0.0199/hour, considering only instance types that have at least 2 vCPUs and 8000 MiB of memory.

At the end of the scan, you will be shown a list of AZs and instance types sorted by price.

Requires a recent version of the [boto3](https://github.com/boto/boto3) library to be installed in your python interpreter.

Tested with Python 3.7


# Copyright
Copyright 2019 Jeshan G. BABOOA

Released under the Simplified BSD Licence. 
