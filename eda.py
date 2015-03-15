#!/usr/bin/env python
#
#
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

data = pd.read_csv('output.pdv', delimiter='|').dropna()
print "Number of rows: %i" % data.shape[0]
print data.head()  # print the first 5 rows
print data.describe()
