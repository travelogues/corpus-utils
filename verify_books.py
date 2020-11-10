import collections
import pandas as pd
from os import listdir, remove
from os.path import isfile, join

BOOKS_FOLDER = '../travelogues-corpus-2020-06-15/18th_century/books'
METADATA_CSV = './reference-csv/TravelogueD18_2020-09-16_inklFoldouts.csv'

# Will delete files that are in the books folder, but not in the CSV
DELETE_NOT_IN_CSV = False

def diff(first, second):
  return [item for item in first if item not in second]

def load_barcode_list():
  df = pd.read_csv(METADATA_CSV, delim_whitespace=False, sep=',', quotechar='"')

  # Barcodes column contains all sorts of things: one or multiple barcodes, nothing at all,
  # one or multiple (non-barcode) URLs, or a mix thereof
  barcodes = df[[ 'Barcode']] 
  barcodes = barcodes.dropna() # Start by removing empty

  as_list = []
  for index, row in barcodes.iterrows():
    codes = [ c.strip() for c in row['Barcode'].split(';') if c.strip().startswith('Z') ]
    as_list = as_list + codes

  return as_list


######################
# Verification script
######################

print (f"Verifying books folder {BOOKS_FOLDER}")

# Read barcodes from folder
in_folder = [f for f in listdir(BOOKS_FOLDER) if isfile(join(BOOKS_FOLDER, f))]
in_folder = set(map(lambda filename: filename.strip()[:-4], in_folder))
print (f"Contains {len(in_folder)} files")

# Read
in_list = load_barcode_list()
in_list_distinct = set(in_list)
print (f"List contains {len(in_list_distinct)} barcodes")

if len(in_list) != len(in_list_distinct):
  print ("Warning - barcode list contains duplicates!")
  print ([item for item, count in collections.Counter(in_list).items() if count > 1])

missing_in_folder = diff(in_list_distinct, in_folder)
not_in_list = diff(in_folder, in_list_distinct)

print(f"Files missing in folder: {len(missing_in_folder)}")
if len(missing_in_folder) > 0:
  print(missing_in_folder)

print(f"Files in folder but not in list: {len(not_in_list)}")
if len(not_in_list) > 0:
  print(not_in_list)
  
  if DELETE_NOT_IN_CSV:
    for f in not_in_list:
      remove(f'{BOOKS_FOLDER}/{f}.txt')

