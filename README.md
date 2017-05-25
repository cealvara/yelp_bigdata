# This is the Amazon's Reviews Project for CS123 at UChicago

## Introduction

This repository contains code to analyze Amazon Product's Review data using Big Data techniques. In particular, we used Parallel Processing (TBC BY MICHAEL), MapReduce and MPI.

## Objectives

1. Learn big data techniques
2. Analyze Amazon products' reviews

## Data description

Data source: http://jmcauley.ucsd.edu/data/amazon/

See **R. He, J. McAuley. Modeling the visual evolution of fashion trends with one-class collaborative filtering. WWW, 2016** and **J. McAuley, C. Targett, J. Shi, A. van den  Hengel. Image-based recommendations on styles and substitutes. SIGIR, 2015** for details.


## Project's tasks:
 - Measure businesses' similarity using reviews
 - 
 - 

## Instructions to run MPI code:
    1. Be sure that "main-disk" is not mounted in read/write mode in some other instance.
    2. SSH into "base-instance-free"
    3. Go to /mnt/local/yelp_bigdata/utilities
    4. Run *python3 start_instances N* (where N is the number of instances to be started)
    5. Run *python3 attach_and_mount.py*
    6. Run *python3 copy_into_instances.py FILENAME1 [FILENAME2 ...]*
    7. SSH into instance-0
    8. Run your command ()
