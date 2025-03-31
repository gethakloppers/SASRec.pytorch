import gzip
import json
from collections import defaultdict
from datetime import datetime
import os

def parse(path):
    g = gzip.open(path, 'r')
    for l in g:
        yield json.loads(l)

countU = defaultdict(lambda: 0)
countP = defaultdict(lambda: 0)
line = 0

dataset_name = 'Beauty2023'
# assert os.path.exists('/Users/gethakloppers/Library/CloudStorage/OneDrive-StellenboschUniversity/A A Meesters/C Data/All_Beauty/All_Beauty.jsonl.gz')
data_path = '/Users/gethakloppers/Library/CloudStorage/OneDrive-StellenboschUniversity/A A Meesters/C Data/All_Beauty_2023/All_Beauty.jsonl.gz'   #2023
# data_path = '/Users/gethakloppers/Downloads/All_Beauty.json.gz' #2018


f = open('reviews_' + dataset_name + '.txt', 'w')
for l in parse(data_path):
    # print(l.keys())
    line += 1
    f.write(" ".join([l['user_id'], l['parent_asin'], str(l['rating']), str(l['timestamp'])]) + ' \n')
    asin = l['parent_asin']
    rev = l['user_id']
    time = l['timestamp']
    countU[rev] += 1
    countP[asin] += 1
f.close()


print(f"Total raw users: {len(countU)}")
print(f"Total raw items: {len(countP)}")


print(f"Users with ≥5 interactions: {sum(1 for v in countU.values() if v >= 5)}")
print(f"Items with ≥5 interactions: {sum(1 for v in countP.values() if v >= 5)}")


usermap = dict()
usernum = 0
itemmap = dict()
itemnum = 0
User = dict()
for l in parse(data_path):
# for l in parse('All_' + dataset_name + '.jsonl.gz'):
    asin = l['parent_asin']
    rev = l['user_id']
    timestamp = l['timestamp']
    
    if countU[rev] < 5 or countP[asin] < 5:
        continue

    if rev in usermap:
        userid = usermap[rev]

    else:
        usernum += 1
        userid = usernum
        usermap[rev] = userid
        User[userid] = []

    if asin in itemmap:
        itemid = itemmap[asin]
    else:
        itemnum += 1
        itemid = itemnum
        itemmap[asin] = itemid
    User[userid].append([timestamp, itemid])

# sort reviews in User according to timestamp
for userid in User.keys():
    User[userid].sort(key=lambda x: x[0])

print(usernum, itemnum)


# Write the final dataset
with open(dataset_name + '.txt', 'w') as f:
    for user in User.keys():
        for i in User[user]:
            f.write('%d %d\n' % (user, i[1]))

# Save usermap and itemmap for recovery
with open(f'{dataset_name}_usermap.json', 'w') as f:
    json.dump(usermap, f)

with open(f'{dataset_name}_itemmap.json', 'w') as f:
    json.dump(itemmap, f)
