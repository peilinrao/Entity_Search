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
def merge_one_file(filename,filetarge):
    temp = {}
    with open(filename,'r',newline = '')as f:
        reader = csv.reader(f)
        for row in reader:
            print(row)
            if row[0] in list(temp.keys()):
                temp[row[0]][0].append(int(row[1]))
                temp[row[0]][1].append(int(row[2]))
            else:
                temp[row[0]] = ([int(row[1])],[int(row[2])])
    for (a,b) in temp.items():
        with open(filetarge,'a',newline='') as f:
            writer=csv.writer(f)
            writer.writerow([a,b[0],b[1]])

# def append_output(list,file):
#     # list: [kId, dId, tf]
#     found = 0
#     with open(file,'w+',newline = '')as f:
#         reader = csv.reader(f)
#         for row in reader:
#             print(row)
#             if row[0] == list[0]:
#
#                 row[1].append(list[1])
#                 row[2].append(list[2])
#                 found = 1
#                 break
#     if(found == 0):
#         with open(file,'a', newline = '')as f:
#             writer=csv.writer(f)
#             writer.writerow([list[0],[list[1]],[list[2]]])
#
# def merge_two_file(file1,file2,filetarge):
#     with open(file1, 'r+',newline = '')as f:
#         reader = csv.reader(f)
#         for row in reader:
#             append_output(row,filetarge)

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
for r, d, f in os.walk("0013wb-88"):
    for fi in f:
        word_list = []
        count += 1
        if count >= 10:
            continue
        print('\x1b[6;30;42m' + os.path.join(r, fi)+'\x1b[0m')
        try:
            with open(os.path.join(r, fi), "r", encoding='utf-8') as f:
                doc= f.read()
            file= BeautifulSoup(doc).get_text()
        except:
            continue
        n_document += 1
        length = len(file)
        with open('D_table.csv','a',newline='') as f:
            writer=csv.writer(f)
            writer.writerow([dId, os.path.join(r, fi), length])
        for i in range(length):
            if file[i] in list_of_letters:
                word += file[i]
            else:
                if word != '':
                    if word not in list(K.values()):
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
        dId += 1

for key, value in K.items():
    idf = math.log(n_document/(1+K_tf[value]))
    K_table.append([key, value,idf])
# Now save K_table to local storage
df = pd.DataFrame(K_table, columns=["kId", "text", "idf"])
df.to_csv('K_table.csv', index=False)
print('\x1b[6;30;42m' + "--------------LOADED--------------"+'\x1b[0m')


# csvsort('KD_tables_1.csv', [0], max_size=10, delimiter='\t')
#
merge_files('KD_tables','KD.csv')
#





#################
# Assumption: E and ED is given
# E_table:[eId, name, cat]
# ED_table: [eId, dId, pos]
#################
#################
# <Generate views: given K_table, KD_table>
#################
# def getV1(k1):
#     V_1 = []
#     for t1, t2 in product(K_table, KD_table):
#         if t1[0] == t2[0] and t1[1] == k1:
#             V_1.append([t2[1],t2[2]])
#     return V_1
#
# def getV2(d1):
#     V_2 = []
#     for t1, t2 in product(D_table, KD_table):
#         if t1[0] == t2[1] and t1[1] == d1:
#             V_2.append([t2[0],t2[2]])
#     return V_2
#
# def getV3(e1):
#     V_3 = []
#     for t1, t2 in product(E_table, ED_table):
#         if t1[0] == t2[0] and t1[1] == e1:
#             V_3.append([t2[0],t2[2]])
#     return V_3
#
# def getV4(k1,k2):
#     V_4_1 = []
#     V_4_2 = []
#     V_4 = []
#     for t1, t2 in product(K_table, KD_table):
#         if t1[0] == t2[0] and t1[1] == k1:
#             V_4_1.append([t2[1],t2[2]])
#     for t1, t2 in product(K_table, KD_table):
#         if t1[0] == t2[0] and t1[1] == k2:
#             V_4_2.append([t2[1],t2[2]])
#     for t1, t2 in product(V_4_1, V_4_2):
#         if t1[0] == t2[0]:
#             V_4.append([t1[0],t1[1],t2[1]])
#     return V_4
#
# def getV5(k1):
#     V_5_1 = []
#     V_5 = []
#     for t1, t2 in product(K_table, KD_table):
#         if t1[0] == t2[0] and t1[1] == k1:
#             V_5_1.append([t2[1],t2[2]])
#     for t1, t2 in product(V_5_1, ED_table):
#         if t1[0] == t2[1] and math.abs(t1[1]-t2[2]) < 20:
#             V_5.append([t1[0],t1[1],t2[2]])
#     return V_5
#
# def getV6(e1, e2):
#     V_6_1 = []
#     V_6_2 = []
#     V_6 = []
#     for t1, t2 in product(E_table, ED_table):
#         if t1[0] == t2[0] and t1[1] == e1:
#             V_6_1.append([t2[1],t2[2]])
#     for t1, t2 in product(E_table, ED_table):
#         if t1[0] == t2[0] and t1[1] == e2:
#             V_6_2.append([t2[1],t2[2]])
#     for t1, t2 in product(V_6_1, V_6_2):
#         if t1[0] == t2[0] and abs(t1[1]-t2[1]) < 20:
#             V_6.append([t1[0],t1[1],t2[1]])
#     return V_6
#
# def getV7(e1):
#     V_7_1 = []
#     V_7 = []
#     for t1, t2 in product(E_table, ED_table):
#         if e1 == t1[1] and t1[0] == t2[0]:
#             V_7_1.append([t2[1],t2[2]])
#     for t1, t2 in product(V_7_1, ED_table):
#         if t1[0] == t2[1] and abs(t1[1]-t2[2]) < 20:
#             V_7.append([t1[0],t1[1],t2[2]])
#     return V_7
#
# def getV8(k1):
#     V_8_1 = []
#     V_8 = []
#     for t1, t2 in product(K_table, KD_table):
#         if t1[0] == t2[0] and t1[1] == k1:
#             V_8_1.append([t2[1],t2[2]])
#     for t1, t2 in product(V_8_1, KD_table):
#         if t1[0] == t2[1] and abs(t1[1]-t2[2]) < 10:
#             V_8.append([t1[0],t1[1],t2[2]])
#     return V_8
#
# def getV9(e1):
#     V_9_1 = []
#     V_9 = []
#     for t1, t2 in product(E_table, ED_table):
#         if e1 == t1[1] and t1[0] == t2[0]:
#             V_9_1.append([t2[1],t2[2]])
#     for t1, t2 in product(V_9_1, KD_table):
#         if t1[0] == t2[1] and abs(t1[1]-t2[2]) < 10:
#             V_9.append([t1[0],t1[1],t2[2]])
#     return V_9


#################
# <test>
#################
# print(getV4("finish","the"))


###
# KD table/ED table/D: put some thershold, flash once reached
# K table/E table: compute dict in memory, flash into disk after wards\
# Merge sort KD by kid/did, ED by eId
# select keyword for whcih V should be stored
