#!/usr/bin/env python
''' This file generates sample data set for research and experimentation of 
detection of anomalous data absences. The idea behind the program is to generate the 
continuous, sample data set that represents claims originating from US states, counties and zip codes.
The program can simulate temporal and/or geographic absences of data. For example, there can be a week
long absence of any data from few counties in the state.
Typical experimental set up is to stream data at some broad, random distribution of zip codes 
'''
__author__ = 'Edmon Begoli'

import sys
import getopt
import argparse
from itertools import chain
from random import randint, sample

accepted_states =["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

def load_arguments():
    parser = argparse.ArgumentParser(description='Load parameters and arguments for data generation of anomalous data streams.')
    parser.add_argument("--num_of_years","-y",metavar='Y', default=2, type=int, nargs='?',
                   help='Number of simulated years.')
    parser.add_argument("--percent_of_missing_counties","-pmc",metavar='PERCENT VALUE', type=int, default=10, nargs='?',
                   help='Percent of the counties that will show missing data.')
    parser.add_argument("--percent_of_missing_days","-pmd",metavar='PERCENT VALUE', type=int, default=10, nargs='?',
                   help='Percent of the days that will not have data.')
    parser.add_argument("--average_temporal_interval","-ati",metavar='PERCENT VALUE', type=int, default=30, nargs='?',
                   help='Average size of the temporal interval of missing data.')
    parser.add_argument("--temporal_continuity","-tc", action="store_true",
                   help='Flag indicating if missing data be temporally contiguous.')
    parser.add_argument("--generating_states", "-s", metavar="STATE ABBR.",default="TN", choices=accepted_states, nargs='*',
                   help='What are the states that generate the data.')

    args = parser.parse_args()
    print("Number of simulated years: {}".format(args.num_of_years))
    print("Percent of missing counties: {}".format(args.percent_of_missing_counties))
    print("Percent (size) of temporal absence: {}".format(args.percent_of_missing_counties))
    print("Are absences temporally contiguous: {}".format(args.temporal_continuity))
    print("Average size of the temporal interval for the missing data (percent of total absence) {}".format(args.average_temporal_interval))
    print("Generating states {}".format(args.generating_states))


# solution adopted from http://stackoverflow.com/questions/27988485/random-selection-of-contiguous-elements-in-an-array-in-python
# recursive division of the sequence
def get_random_division(lst, minsize, maxsize):
    split_index = randint(minsize, maxsize)
    # if the remaining list would get too small, return the unsplit one
    if minsize>len(lst)-split_index:
        return [lst]
    return [lst[:split_index]] + get_random_division(lst[split_index:], minsize, maxsize)


def get_gap_map(total_days):

    #this is a 1...days representation
    day_map = [i+1 for i in range(total_days)]

    days = range(0, total_days)
    # determine size range of the subdivisions - min 5 days, max - 15% of the total interval
    minsize, maxsize = 5, int(0.15*len(day_map))
    # choose three of the subdivided sequences
    periods = sample(get_random_division(day_map, minsize, maxsize), 3)
    # pick 3 10-day periods randomly without overlapping

    print(periods)

    print day_map
    rand_days = chain(*periods)
    for day_gap in list(rand_days):
        print "assigning: ", (day_gap-1), " to list of size: ", len(days)
        days[day_gap-1] = 0
    print(list(rand_days))
    print days

def create_random_time_interval_gaps(days, gaps_as_percent, avg_gap_as_percent):
    """ Function creates a list of intervals of the average size around avg_size. """
    
    print "days ",days
    #first find the total number of gap days by calculating it as percent of the total days
    total_size_of_missing_days = round(days * (float(gaps_as_percent)/100))
    print "total size of missing days ", total_size_of_missing_days
    #now, calculate the average gap size as percent of the total number of gap days
    average_gap_in_days = round(total_size_of_missing_days * (float(avg_gap_as_percent)/100)) 
    num_of_time_gaps = round(float(days)/average_gap_in_days)

    #number of fills is a space around the gaps, from day 0 to last day
    # there are num_of_gaps + 1
    num_of_time_fills = num_of_time_gaps + 1
    size_of_fill = int(round((float(days)-total_size_of_missing_days)/num_of_time_fills))
    
    print "num_of_time_gaps ", num_of_time_gaps
    print "num_of_time_files ", num_of_time_fills
    print "size_of_fill ", size_of_fill

    start_indices = []
    begin = 0
    for i in range(0,int(num_of_time_gaps)):
        begin = randint(begin,int(size_of_fill)-1)
        start_indices.append(begin)
        begin = begin + size_of_fill
        if begin + average_gap_in_days > total_size_of_missing_days:
           break

     
    print start_indices 
    print "num of time gaps ", num_of_time_gaps   
    print "num of time fills ", num_of_time_fills 

    interval_tuples = []
    credit_space = days - total_size_of_missing_days
    start = 0
    end = randint(0,avg_size)
    #algorithm - take the size of present data and, using it as a reminder, create a distribution of days

if __name__ == '__main__':
    print get_gap_map(365)
    print "done!"


