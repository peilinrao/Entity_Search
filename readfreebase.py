import gzip
import csv
import heapq

try:
    os.remove("CAT.csv")
except:
    pass
count = 0
stat = {}
IGNORE = ['common', 'base', 'type','media_common','dataworld','freebase', 'user', 'pipeline', 'cvg', 'protected_sites', 'distilled_spirits', 'kp_lw','metropolitan_transit']
# list_of_eId = []
# with open('ED.csv','r',newline = '') as f:
#     reader = csv.reader(f)
#     for row in reader:
#         if row[0] not in list_of_eId:
#             list_of_eId.append(row[0])

# with open('CAT.csv','a',newline='') as f:
#     writer=csv.writer(f)
#     with gzip.open("freebase-rdf-latest.gz","rt") as f:
#         for line in f:
#             line = line.split()
#             count+=1
#             if(count%10000==0):
#                 print(count)
#             if line[1] == '<http://rdf.freebase.com/ns/type.object.type>':
#                 line[0] = line[0][27:-1].replace(".","/")
#                 line[2]=line[2][28:-1]
#                 print(line[0],line[2])
#                 if line[2] == "common.topic" or line[2][0:4] == "base":
#                     continue
#                 if line[0] in list_of_eId:
#                     print(line[0],line[2])
#                     writer.writerow([line[0],line[2]])
count += 0

with open('CAT.csv','a',newline='') as f:
    writer=csv.writer(f)
    with gzip.open("freebase-rdf-latest.gz","rt") as f:
        for line in f:
            count+=1
            if(count%100000==0):
                print(count)
            if count >= 500000000:
                break
            line = line.split()
            if line[1] == '<http://rdf.freebase.com/ns/type.object.type>':
                line[0] = line[0][27:-1].replace(".","/")
                line[2]=line[2][28:-1]
                ent = line[2].split('.')[0]
                if ent not in IGNORE:
                    writer.writerow([line[0],line[2]])
