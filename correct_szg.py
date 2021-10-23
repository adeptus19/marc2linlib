import re
import json
nyelv = [
    ("hun" , "magyar"),
    ("cro" , "horvát"),
    ("eng" , "angol"),
    ("ger" , "német"),
    ("fre", "francia"),
    ("rus", "orosz")
 ]
dokt = [
    ("BOOK", "Könyv"),
    ("CD", "Hangzó"),
    ("DVD", "AV"),
    ("VIDEO", "AV"),
    ("AUDIO", "Hangzó"),
    ("CDR", "Multimédia"),
    ("MANUS", "Könyv"),
    ("ISSBD", "Folyóirat")
]
def rec_sorted(record) :
    temprec = dict()
    tszodata=list()
    kozrem = list()
    result = dict()
    pldcount = 0
    plddata = list()
    for row in record :
            row_1 = row[0:4]
            key = re.findall("(\$.)[^$^\n]+",row)
            value = re.findall("\$.([^$^\n]+)",row)
    #        print(key, value)
            if row_1 == "=920" :
                pldcount = pldcount + 1
                plddata.append([key, value])
            elif row_1 == "=653":
                tszodata.append(value[0])
            elif row_1 == "=960":
                kozrem.append([key, value])
            else :
                temprec[row_1]= dict()
                prevkey=""
                for i in range(len(key)) :
                       if key[i] == prevkey :
                            if prevkey !="" :
                                temprec[row_1][key[i]] = temprec[row_1][key[i-1]] + ";" + value[i]
                       else:
                            temprec[row_1][key[i]]=value[i]
                            prevkey = key[i]
    result["book"] = temprec
    result["exempl"] = plddata
    result["tszo"] = tszodata
    result["kozrem"] = kozrem
    return result
def setupdate(record):
    peldany = record['exempl'][0]
    vkpos = peldany[0].index("$5")
    vkod = peldany[1][vkpos]
    updatesql = "UPDATE kpld SET lbetu = 'SZG' WHERE vonalkod ='" + vkod + " ';"
    print(updatesql)
    line = updatesql + "\n"
    outputf.write(line)
    return record
file_to_process = '../full_l2.mrk'
filestruct = open("linlib_struct.json")
linlib_struct = json.load(filestruct)
filestruct.close()
outputf = open("correct_szg.sql","w", encoding="latin2")
try :
    fhandle = open(file_to_process, "r", encoding = "latin2")
except:
    print("Error by opening file")
    quit()
read_lines = 0
read_records = 0
act_record = list()
last_record = list()
rec_len = 0
konyv_id = 0
change = 0
for line in fhandle :
    read_lines = read_lines + 1
    if len(line.strip()) > 0 :
        rec_len = rec_len + 1
        act_record.append(line)
    elif rec_len > 0 :
        read_records = read_records + 1
        last_record = act_record
        act_record = []
        testrec = rec_sorted(last_record)
        katstr = ''
        try:
            katstr = testrec['book']['=852']['$h'][:3]
        except:
            pass
        if katstr == "SZG":
            setupdate(testrec)
            change = change + 1
print(change)
fhandle.close()
outputf.close()