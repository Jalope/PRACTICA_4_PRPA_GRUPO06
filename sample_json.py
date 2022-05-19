#Credit to: Russell Jurney @rjurney https://gist.github.com/rjurney/5e926262041dc1475f0dd8b2743d6ad5
import sys, os, re
import json
import numpy as np
import math

with open("jun.json",encoding = "ISO-8859-1") as f:
  records = [json.loads(x) for x in f]

count = len(records)
sample_ratio = 0.005
sample_count = math.ceil(count * sample_ratio)

sample_indexes = np.random.choice(
  count,
  sample_count
)

sample_records = []
for sample_index in sample_indexes:
  sample_record = records[sample_index]
  sample_records.append(sample_record)

assert len(sample_records) == sample_count

with open("sample02.json", "w") as f:
  for record in sample_records:
    f.write(json.dumps(record) + "\n")

print("Sampled {} records from {} original records.".format(
  sample_count,
  count
))

	
