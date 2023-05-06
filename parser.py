lines = [line.split() for line in open("/Users/hasansendani/PycharmProjects/trader/access.log", "r").read().split('\n')]
import json
khiar = []
for line in lines:
    try:
        khiar.append(line[4])
    except:
        pass


mowz = {}

for tokhm in khiar:
    if tokhm in mowz.keys():
        mowz[tokhm] += 1
    else:
        mowz[tokhm] = 1

print(json.dumps(dict(sorted(mowz.items(), key=lambda item: item[1])), indent=3))
