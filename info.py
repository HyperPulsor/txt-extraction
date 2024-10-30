import chardet
import os

input_folder = 'input'

with open('VW_FORMG0002.txt', 'rb') as f:
    for line in f:
        data = line
        break

# Step 3: Detect Encoding using chardet Library
encoding_result = chardet.detect(data)

# Step 4: Retrieve Encoding Information
encoding = encoding_result['encoding']

# Step 5: Print Detected Encoding Information
print(f"Encoding Type: {encoding}")
