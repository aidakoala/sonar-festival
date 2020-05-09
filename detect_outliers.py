import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import sys

anomalies = []

# Function to Detection Outlier on one-dimentional datasets.
def find_anomalies(data):
    # Set upper and lower limit to 3 standard deviation
    data_std = np.std(data)
    data_mean = np.mean(data)
    anomaly_cut_off = data_std * 3

    lower_limit  = data_mean - anomaly_cut_off 
    upper_limit = data_mean + anomaly_cut_off
    print(lower_limit)
    # Generate outliers
    for outlier in data:
        if outlier > upper_limit or outlier < lower_limit:
            anomalies.append(outlier)
    return anomalies

def main():
	durations = pd.read_csv(sys.argv[1])
	# check if the data has a normal distribution
	max_val = max(durations['duration'])
	print(max_val)
	# esantionare la 20 de min
	plt.hist(durations['duration'], color = 'blue', edgecolor = 'black', bins = max_val / 20)
	plt.title('Histogram of the time spent by a node at the festival day 3')
	plt.xlabel('time spent at the festival')
	plt.ylabel('nodes')
	plt.show()

	# print(find_anomalies(durations['duration']))

main()
