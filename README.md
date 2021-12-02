# metastock2pd

A simple utility to convert metastock data to pandas dataframe.

You can conda install it from https://anaconda.org/yoavgit/metastock2pd

This is a simpler version of https://github.com/themech/ms2txt and themech should get all the credit for the heavy lifting in parsing the bytes.

There are two functions you care about: 

- metastock_read_master('c:/directory') will read metastock master, emaster and xmaster files as a table of available files. We can then read these files directly using...
- metastock_read('c:/directory/F1.dat') will read metastock datafiles: either dat or mwd extensions.


