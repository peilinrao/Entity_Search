import numpy as np
import string
from bs4 import BeautifulSoup
import re
import os
import math
from itertools import product
from csvsorter import csvsort
from collections import defaultdict
import pandas as pd
import csv
import ast
import shutil
import nltk
import heapq

UPPER_NUM_OF_FILE = 10
NUM_OF_TUPLES_TO_INSERT = 10
# Change PATH_TO_DOC to the folders that have docs in it
PATH_TO_DOC = "0013wb-88"
# Change the corresponding PATH_TO_ENTITY to the folders that have entities in it
PATH_TO_ENTITY = "1300wb-88.anns.tsv"

stop_words = {'ourselves', 'hers', 'between', 'yourself', 'but', 'again', 'there', 'about', 'once', 'during', 'out', 'very', 'having', 'with', 'they', 'own', 'an', 'be', 'some', 'for', 'do', 'its', 'yours', 'such', 'into', 'of', 'most', 'itself', 'other', 'off', 'is', 's', 'am', 'or', 'who', 'as', 'from', 'him', 'each', 'the', 'themselves', 'until', 'below', 'are', 'we', 'these', 'your', 'his', 'through', 'don', 'nor', 'me', 'were', 'her', 'more', 'himself', 'this', 'down', 'should', 'our', 'their', 'while', 'above', 'both', 'up', 'to', 'ours', 'had', 'she', 'all', 'no', 'when', 'at', 'any', 'before', 'them', 'same', 'and', 'been', 'have', 'in', 'will', 'on', 'does', 'yourselves', 'then', 'that', 'because', 'what', 'over', 'why', 'so', 'can', 'did', 'not', 'now', 'under', 'he', 'you', 'herself', 'has', 'just', 'where', 'too', 'only', 'myself', 'which', 'those', 'i', 'after', 'few', 'whom', 't', 'being', 'if', 'theirs', 'my', 'against', 'a', 'by', 'doing', 'it', 'how', 'further', 'was', 'here', 'than'}


try:
    os.remove("EN.csv")
except:
    pass
try:
    os.remove("D2.csv")
except:
    pass
try:
    os.remove("KDr.csv")
except:
    pass
try:
    os.remove("KD.csv")
except:
    pass
try:
    os.remove("D.csv")
except:
    pass
try:
    os.remove("K.csv")
except:
    pass
try:
    shutil.rmtree("KD_tables")

except:
    pass
try:
    os.remove("ED.csv")
except:
    pass
try:
    os.remove("E.csv")
except:
    pass

os.mkdir("KD_tables")


columns = defaultdict(list)

count = 0
thisdir = os.getcwd()
K = {} # A dict of kId:text
K_tf = {} # word:appearance
K_dtf = {} # (word, dId):appearance
K_table = [] # [kId, text, idf]
KD_table = [] # [kId, dId, pos]
K_T = {}

kId = 0
dId = 0
n_document = 0 # how many documents have we seen
word = ''
list_of_letters = list(string.ascii_lowercase)+list(string.ascii_uppercase)

#################
# helper
#################
def get_line_info(row):
    num_t = 0
    A = ""
    B = ""
    C = ""
    D = ""
    E = ""
    F = ""
    G = ""
    H = ""
    for i in range(len(row)):
        #clueweb12-1300wb-88-36257
        if(row[i] == "\t"):
            num_t += 1
            continue

        if num_t == 0:
            A += row[i]
        if num_t == 1:
            B += row[i]
        if num_t == 2:
            C += row[i]
        if num_t == 3:
            D += row[i]
        if num_t == 4:
            E += row[i]
        if num_t == 5:
            F += row[i]
        if num_t == 6:
            G += row[i]
        if num_t == 7:
            H += row[i]

    list = [A,B,C,D,E,F,G,H]
    return list

def merge_one_file(filename,filetarge):
    temp = {}
    with open(filename,'r',newline = '')as f:
        reader = csv.reader(f)
        for row in reader:
            if row[0] in list(temp.keys()):
                temp[row[0]][0].append((row[1]))
                temp[row[0]][1].append((row[2]))
            else:
                temp[row[0]] = ([(row[1])],[(row[2])])
    for (a,b) in temp.items():
        with open(filetarge,'a',newline='') as f:
            writer=csv.writer(f)
            writer.writerow([a,b[0],b[1]])

def merge_files(foldername, targetfile):
    with open(targetfile, 'w+',newline = '')as f:
        pass # create targetfile
    for filename in os.listdir(thisdir+"/KD_tables"):
        with open("KD_tables/"+filename, 'r+', newline = '') as f:
            try:
                reader = csv.reader(f)
                for row in reader:
                    found = 0
                    temp = [] #kId, dId[], tf[]
                    with open(targetfile, 'r+', newline = '') as tf, open("temp.csv", 'w+', newline = '') as te:
                        tfreader = csv.reader(tf)
                        tewriter = csv.writer(te)
                        for tfrow in tfreader:
                            if tfrow[0] == row[0]:
                                found = 1
                                temp = row
                                temp[1]+=","+(tfrow[1])
                                temp[2]+=","+(tfrow[2])
                                tewriter.writerow(temp)
                            else:
                                tewriter.writerow(tfrow)
                        if(found == 0):
                            tewriter.writerow([row[0],row[1],row[2]])

                    os.remove(targetfile)
                    os.rename("temp.csv", targetfile)
            except:
                continue



#################
# <Loading tables>
#################
dId = 0
# r=root, d=directories, f = files
for r, d, f in os.walk(PATH_TO_DOC):
    for fi in f:
        firstword = ""
        word_list = []
        count += 1
        if count >= UPPER_NUM_OF_FILE:
            continue
        print('\x1b[6;30;42m' + os.path.join(r, fi)+'\x1b[0m')
        try:
            with open(os.path.join(r, fi), "r", encoding='utf-8') as f:
                doc= f.read()
            file= BeautifulSoup(doc).get_text()
            # Cleaning:
        except:
            continue
        file = re.sub('[^A-Za-z0-9 ]+', '', file).lower().split()
        n_document += 1
        length = len(file)
        with open('D.csv','a',newline='') as f:
            writer=csv.writer(f)
            writer.writerow([dId, os.path.join(r, fi), length])
        for word in file:
            if word not in list(K.values()):
                if len(K.values())==0:
                    firstword = word
                K[kId] = word
                K_T[word] = kId
                kId += 1
            if word not in word_list:
                word_list.append(word)
            if (word, dId) not in list(K_dtf.keys()):
                K_dtf[(word, dId)] = 1
            else:
                K_dtf[(word, dId)] +=1
            if word not in list(K_tf.keys()):
                K_tf[word] = 1
            else:
                K_tf[word] += 1
            word = ''

        for word in word_list:
            with open('KD_tables/'+str(count)+'.csv','a',newline='') as f:
                writer=csv.writer(f)
                writer.writerow([int(K_T[word]),int(dId),int(K_dtf[(word, dId)])])
        list_kId = []
        list_tf = []
        for word in word_list:
            list_kId.append(K_T[word])
            list_tf.append(K_dtf[(word, dId)])
        with open('KDr.csv','a',newline='') as f:
            writer=csv.writer(f)
            writer.writerow([dId,list_kId,list_tf])
        with open('D2.csv','a',newline='') as f:
            writer=csv.writer(f)
            writer.writerow([dId,firstword,file])
        dId += 1

for key, value in K.items():
    idf = math.log(n_document/(1+K_tf[value]))
    K_table.append([key, value,idf])
# Now save K_table to local storage
df = pd.DataFrame(K_table, columns=["kId", "text", "idf"])
df.to_csv('K.csv', index=False)
print('\x1b[6;30;42m' + "--------------LOADED--------------"+'\x1b[0m')


# csvsort('KD_tables_1.csv', [0], max_size=10, delimiter='\t')
#
merge_files('KD_tables','KD.csv')
#

#################
# Creating E and ED tables
#################



ED_dict = {} # eId, name, cat
Ent_lst = []
E_dtf = {}
E_tf = {}
Dict_EID_DOC = {} # (eId, dId, IDF)
List_Doc = []
Dict_name = {}
count = 0

with open("1300wb-88.anns.tsv",'r',newline = '')as f:
    reader = csv.reader(f)
    for row in reader:

        # count += 1
        # if(count > UPPER_NUM_OF_ENTITY):
        #     continue

        temp = get_line_info(row[0])
        temp[0] = temp[0][-5:]
        holder = ''
        flag_read = 0
        for i in range(len(temp[0])):
            if temp[0][i] is not '0':
                flag_read = 1
            if flag_read == 1:
                holder+=temp[0][i]

        if holder == '':
            temp[0] = '0'
        else:
            temp[0] = holder
        temp[2] = re.sub(r'[^A-Za-z0-9 ]+', '', temp[2]).strip().lower().split()
        if len(temp[2])>1:
            continue
        else:
            temp[2] = temp[2][0]
        if temp[0] not in List_Doc:
            List_Doc.append(temp[0])
        if (temp[7]) not in list(Dict_EID_DOC.keys()):
            Dict_EID_DOC[temp[7]]=[temp[0]]
        else:
            Dict_EID_DOC[temp[7]].append(temp[0])
        # print(temp[0])
        if temp[2] not in list(Dict_name.keys()):
            Dict_name[temp[2]] = temp[7]

        if temp[7] not in list(ED_dict.keys()):
            ED_dict[temp[7]] = temp[2]
            with open('E.csv','a',newline='') as f:
                writer=csv.writer(f)
                writer.writerow([temp[7], temp[2], count%6])
        if temp[7] not in Ent_lst:
            Ent_lst.append(temp[7])
        if (temp[7], temp[0]) not in list(E_dtf.keys()):
            E_dtf[(temp[7], temp[0])] = 1
        else:
            E_dtf[(temp[7], temp[0])] += 1
        if temp[7] not in list(E_tf.keys()):
            E_tf[temp[7]] = 1
        else:
            E_tf[temp[7]] += 1

        with open('temp.csv','a',newline='') as f:
            writer=csv.writer(f)
            writer.writerow([temp[7],temp[0],E_dtf[(temp[7], temp[0])]])

for name in (list(Dict_name.keys())):
    with open('EN.csv','a',newline='') as f:
        writer=csv.writer(f)
        writer.writerow([Dict_name[name],name,math.log(len(List_Doc)/len(Dict_EID_DOC[Dict_name[name]]))])


merge_one_file('temp.csv', 'ED.csv')
os.remove("temp.csv")


# KK
def getKK(k1):
    keyword = []
    heapq.heapify(keyword)
    kId = -1
    with open('K.csv','r',newline='') as f:
        reader=csv.reader(f)
        for row in reader:
            if row[1] == k1:
                kId = row[0]
                break
    with open('KD.csv','r',newline = '') as f:
        reader=csv.reader(f)
        for row in reader:
            if row[0] == kId:
                dId = row[1].split(",")
                for every_dId in dId:
                    every_dId = re.sub('[\[\]]', '', every_dId)
                    with open('KD.csv','r',newline = '') as m:
                        reader_other=csv.reader(m)
                        for row_other in reader_other:
                            dId_other = row_other[1].split(",")
                            if every_dId in dId_other:
                                temp_kId = row_other[0]
                                word = K[int(temp_kId)]
                                if word not in list(item for _, item in keyword):
                                    priority = K_tf[word]/len(dId_other)
                                    heapq.heappush(keyword, (priority, word))
                                    if len(list(item for _, item in keyword))>n:
                                        keyword=heapq.nlargest(NUM_OF_TUPLES_TO_INSERT, keyword)

    return keyword

def getKE(k1):
    keyword = []
    heapq.heapify(keyword)
    kId = -1
    with open('K.csv','r',newline='') as f:
        reader=csv.reader(f)
        for row in reader:
            if row[1] == k1:
                kId = row[0]
                break
    with open('KD.csv','r',newline = '') as f:
        reader=csv.reader(f)
        for row in reader:
            if row[0] == kId:
                dId = row[1].split(",")
                for every_dId in dId:
                    every_dId = re.sub('[\[\]]', '', every_dId)
                    with open('ED.csv','r',newline = '') as m:
                        reader_other=csv.reader(m)
                        for row_other in reader_other:
                            dId_other = row_other[1].split(",")
                            if str(every_dId) in dId_other:
                                temp_eId = row_other[0]
                                if temp_eId not in list(item for _, item in keyword):
                                    priority = E_tf[temp_eId]/len(dId_other)
                                    heapq.heappush(keyword, (priority, temp_eId))
                                    if len(list(item for _, item in keyword))>n:
                                        keyword=heapq.nlargest(NUM_OF_TUPLES_TO_INSERT, keyword)

    return keyword

def getEK(e1):
    keyword = []
    heapq.heapify(keyword)
    eId = -1
    with open('E.csv','r',newline='') as f:
        reader=csv.reader(f)
        for row in reader:
            if row[1] == e1:
                eId = row[0]
                break
    with open('ED.csv','r',newline = '') as f:
        reader=csv.reader(f)
        for row in reader:
            if row[0] == eId:
                dId = row[1].split(",")
                for every_dId in dId:
                    every_dId = re.sub('[\[\]]', '', every_dId)
                    with open('KD.csv','r',newline = '') as m:
                        reader_other=csv.reader(m)
                        for row_other in reader_other:
                            dId_other = row_other[1].split(",")
                            print("This:",every_dId)
                            print("That:",dId_other)
                            if every_dId in dId_other:
                                print("Correct here")
                                temp_kId = row_other[0]
                                word = K[int(temp_kId)]
                                if word not in list(item for _, item in keyword):
                                    priority = K_tf[word]/len(dId_other)
                                    heapq.heappush(keyword, (priority, word))
                                    if len(list(item for _, item in keyword))>n:
                                        keyword=heapq.nlargest(NUM_OF_TUPLES_TO_INSERT, keyword)
    return keyword

def getEE(e1):
    keyword = []
    heapq.heapify(keyword)
    kId = -1
    with open('E.csv','r',newline='') as f:
        reader=csv.reader(f)
        for row in reader:
            if row[1] == e1:
                eId = row[0]
                break
    with open('ED.csv','r',newline = '') as f:
        reader=csv.reader(f)
        for row in reader:
            if row[0] == eId:
                dId = row[1].split(",")
                for every_dId in dId:
                    every_dId = re.sub('[\[\]]', '', every_dId)
                    with open('ED.csv','r',newline = '') as m:
                        reader_other=csv.reader(m)
                        for row_other in reader_other:
                            dId_other = row_other[1].split(",")
                            if every_dId in dId_other:
                                temp_eId = row_other[0]
                                if temp_eId not in list(item for _, item in keyword):
                                    priority = E_tf[temp_eId]/len(dId_other)
                                    heapq.heappush(keyword, (priority, temp_eId))
                                    if len(list(item for _, item in keyword))>n:
                                        keyword=heapq.nlargest(NUM_OF_TUPLES_TO_INSERT, keyword)
    return keyword


# In order to get EE, EK, KE, KK,
# use getEE("entity"),getEK("entity"),getKE("keyword"),getKK("keyword")
