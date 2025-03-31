import gzip
from collections import defaultdict
from datetime import datetime
import json

def parse(path):
    g = gzip.open(path, 'r')
    for l in g:
        yield json.loads(l)

countU = defaultdict(lambda: 0)
countP = defaultdict(lambda: 0)
line = 0

data_path = '/Users/gethakloppers/Library/CloudStorage/OneDrive-StellenboschUniversity/A A Meesters/C Data/Beauty/reviews_Beauty.json.gz'  

dataset_name = 'Beauty'
f = open('reviews_' + dataset_name + '.txt', 'w')
for l in parse(data_path):
    # print(l.keys())
    line += 1
    f.write(" ".join([l['reviewerID'], l['asin'], str(l['overall']), str(l['unixReviewTime'])]) + ' \n')
    asin = l['asin']
    rev = l['reviewerID']
    time = l['unixReviewTime']
    countU[rev] += 1
    countP[asin] += 1
f.close()


# # VIEW META DATA
# data_path2 = '/Users/gethakloppers/Downloads/meta_All_Beauty.json.gz'
# for l in parse(data_path2):
#     print(l.keys())


print(f"Total raw users: {len(countU)}")
print(f"Total raw items: {len(countP)}")


print(f"Users with ≥5 interactions: {sum(1 for v in countU.values() if v >= 5)}")
print(f"Items with ≥5 interactions: {sum(1 for v in countP.values() if v >= 5)}")


usermap = dict()
usernum = 0
itemmap = dict()
itemnum = 0
User = dict()
for l in parse('reviews_' + dataset_name + '.json.gz'):
    line += 1
    asin = l['asin']
    rev = l['reviewerID']
    time = l['unixReviewTime']
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
    User[userid].append([time, itemid])
# sort reviews in User according to time

for userid in User.keys():
    User[userid].sort(key=lambda x: x[0])

print(usernum, itemnum)


# Write the final dataset
f = open('Beauty.txt', 'w')
for user in User.keys():
    for i in User[user]:
        f.write('%d %d\n' % (user, i[1]))
f.close()


# Save usermap and itemmap for recovery
with open(f'{dataset_name}_usermap.json', 'w') as f:
    json.dump(usermap, f)

with open(f'{dataset_name}_itemmap.json', 'w') as f:
    json.dump(itemmap, f)
