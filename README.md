# Travelogues Corpus Utilities

Utility scripts for building and maintaining the Travelogues text corpus.

## verify_books

A simple helper utility for verifying a folder of .txt files against the Travelogues CSV tables. 
The script reads the configured CSV (assuming the usual Travelogues conventions), extracts
the barcodes, and checks against a folder of text files.

As result, the script reports:

1. Which barcodes mentioned in the CSV are missing from the folder
2. Which barcode files contained in the folder are not mentioned in the CSV

Optionally, the script can delete the files identified in step 2.

## sacha_download

Jan's original script for mass-downloading a list of books from the ONB Sacha IIIF endpoint. 