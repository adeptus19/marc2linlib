import psycopg2
import re
import json
file_to_process = input("MARC-21 file to process: ")
filestruct = open("linlib_struct.json")
linlib_struct = json.load(filestruct)
filestruct.close()
try :
    fhandle = open(file_to_process, "r", encoding = "utf-8")
except:
    print("Error by opening file")
    quit()
read_lines = 0
read_records = 0
act_record = list()
last_record = list()
rec_len = 0
konyv_id = 0
def gettipus(inp1, inp) :
    tipus = 0
    for row in linlib_struct :
       for field in linlib_struct[row] :
                if linlib_struct[row][field]["field"] == inp and linlib_struct[row][field]["table"]  == inp1 :
                    tipus = linlib_struct[row][field]["type"]
    return tipus
def sanitize(record):
    for row in record :
        tipus = gettipus(row["table"],row["column"])
        if tipus == 256:
            row["value"]=1000
        elif tipus == 258:
            try:
                row["value"][0]=int(row["value"][0][2:])
            except:
                row["value"][0]= 0
        elif tipus > 0 and tipus < 256:
            row["value"][0] = row["value"][0][:tipus]
            row["value"][0] = row["value"][0].replace("\'","''")
        else:
            row["value"][0] = row["value"][0].replace("\'","''")
    return record
def setszerzo(record):
    szname = ""
    for v in record :
        if v["column"] == "szerzo":
            szname = v["value"][0]
            v["value"][0] = v["value"][0].split(";")[0]
            v["value"][0] = v["value"][0].replace("\'","''")
            break
    record.append({"table" : "konyv", "column":"szerad" , "value" :[szname]})
    return record
def setkcim(record):
    szname = ""
    for v in record :
        if v["column"] == "fc":
            szname = v["value"][0][:40]
            print(szname)
    record.append({"table" : "kcim", "column":"cim" , "value" :[szname]})
    return record
def setkszerzo(record):
    szname = ""
    for v in record :
        if v["column"] == "szerzo":
            szname = v["value"][0][:35]
           # szname = szname.replace("\'","''")

    record.append({"table" : "kszerzo", "column":"szerzo" , "value" :[szname]})
    return record
def rec_sorted(record) :
    temprec = dict()
    data=dict()
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
    return result
def get_konyv_id() :
    query = "SELECT COUNT(*) FROM konyv;"
    dbc.execute(query)
    res=dbc.fetchone()
    if res is not None:
        x=int(res[0])+1
    else :
        x=1
    return x
def get_sorszam(tablename):
    query = "SELECT COUNT(*) FROM " + tablename + ";"
    dbc.execute(query)
    res=dbc.fetchone()
    if res is not None:
        x=int(res[0])+1
    else :
        x=1
    return x
def save_rec(record_full) :
    # INSERT INTO konyv (id, fc, ac, szerzo, ar, isbn, nyelv, eto, szerad, kiadas, kiadjel, nyomda, ter, tmt, tszo, szakj, raktj)
    # VALUES (AI, record_s["=245"]["$a"]), record_s["=245"]["$b"], record_s["=245"]["$c"] ...
    record_s = record_full["book"]
    values = list()
    value = dict()
    target_value = list()
    pldcount = 0
    konyv_id = get_konyv_id()
    for row in record_s :
            for field in record_s[row]:
                target_value=[]
                try:
                    target_table = linlib_struct[row][field]["table"]
                    target_column = linlib_struct[row][field]["field"]
                except:
                    continue
                target_value.append(record_s[row][field])
                values.append({"table": target_table, "column":target_column,"value":target_value})
    values = setszerzo(values)
    values = setkszerzo(values)
    values = setkcim(values) 
    values = sanitize(values)
    insertsql = dict()
    valuessql = dict()
    insertsql["konyv"] = "INSERT INTO konyv (id, fc, ac, szerzo, isbn, nyelv, eto, szerad, kiadas, kiadjel, nyomda, ter, tmt, tszo, szakj, raktj) VALUES ("
    valuessql["konyv"] = "&@id, &@fc, &@ac, &@szerzo, &@isbn, &@nyelv, &@eto, &@szerad, &@kiadas, &@kiadjel, &@nyomda, &@ter, &@tmt, &@tszo, &@szakj, &@raktj,"
    insertsql["kszerzo"] = "INSERT INTO kszerzo (sorszam, doktipus, id, szerzo ) VALUES ("    
    valuessql["kszerzo"] = "&@sorszam, &@doktipus, &@id, &@szerzo,"
    insertsql["kcim"] = "INSERT INTO kcim (sorszam, doktipus, id, cim ) VALUES ("    
    valuessql["kcim"] = "&@sorszam, &@doktipus, &@id, &@cim,"
    insertsql["kpld"] = "INSERT INTO kpld (sorszam, id, lhely, lszam) VALUES ("    
    valuessql["kpld"] = "&@sorszam, &@id, &@lhely, &@lszam"
    for item in values :
        repfrom = "&@" + item["column"]
        try:
            repto = "\'" + item["value"][0] + "\'" 
        except:
            repto = str(item["value"]) 
        valuessql[item["table"]] = valuessql[item["table"]].replace(repfrom, repto)
    valuessql["konyv"] = valuessql["konyv"].replace("&@id", str(konyv_id))
    valuessql["kszerzo"] = valuessql["kszerzo"].replace("&@id", str(konyv_id))
    valuessql["kszerzo"] = valuessql["kszerzo"].replace("&@sorszam", str(get_sorszam("kszerzo")))
    valuessql["kcim"] = valuessql["kcim"].replace("&@id", str(konyv_id))
    valuessql["kcim"] = valuessql["kcim"].replace("&@sorszam", str(get_sorszam("kcim")))
    
    for key in valuessql :
        s = re.compile("&@[^,.]+,?")
        x = s.sub("' ',", valuessql[key])
        valuessql[key] = x.rstrip(",") + ");"
    queries = list()
    queries.append(insertsql["konyv"] + valuessql["konyv"])
    queries.append(insertsql["kszerzo"] + valuessql["kszerzo"])
    queries.append(insertsql["kcim"] + valuessql["kcim"])
    exemplars = list()
    book_column = list()
    book_columns = list()
    book_value = list()
    book_values = list()
    valuessql["kpld"] = "&@sorszam, &@id, &@lhely, &@lszam"
    i = 0
    count = 0
    for book in record_full["exempl"] :
            query1 = valuessql["kpld"]
            index = list()
            pos = 0
            for dollar in book[0] :
                try:
                    book_column.append(linlib_struct["=920"][dollar]["field"])
                    index.append(pos)
                    pos = pos +1
                except:
                    pos = pos +1
                    continue
            count = count + 1
    for j in range(count):
        tempcs = list()
        tempvs = list()
        for i in range(len(index)):
            tempcs.append(book_column[i])
            tempvs.append([record_full["exempl"][j][1][index[i]]])
        book_columns.append(tempcs)
        book_values.append(tempvs)
    pldvals = list()
    pldvalue = dict()
    ct1 = 0
    for plds in book_columns:
        ct2 = 0
        pldvalues = list()
        for pld in plds :
            pldvalue["table"] = "kpld"
            pldvalue["field"] = pld
            pldvalue["value"] = book_values[ct1][ct2][0]
            if pld == "lszam":
                print(book_values[ct1][ct2])
                try:
                    pldvalue["value"] = str(int((book_values[ct1][ct2][0][2:])))
                except:
                    pldvalue["value"] = "0"
            ct2 = ct2 + 1
            pldvalues.append({"table":pldvalue["table"],"column" : pldvalue["field"],"value" : pldvalue["value"]})
        pldvals.append(pldvalues)
        ct1 = ct1 + 1
    ct=0
    for i in pldvals:
        query1 = valuessql["kpld"]
        for j in i:
            repfrom = "&@" + j["column"]
            try:
                repto = "\'" + j["value"] + "\'"
            except:
                repto = str(j["value"])
            query1 = query1.replace(repfrom,repto)
            ssz = get_sorszam("kpld") + ct
            query1 = query1.replace("&@sorszam", str(ssz))
            query1 = query1.replace("&@id", str(konyv_id))
        s = re.compile("&@[^,.]+,?")
        x = s.sub("0", query1)
        query1 = x
        queries.append(insertsql["kpld"]+(query1+");"))    
        ct = ct + 1
    for q in queries :
        print(q)
        dbc.execute(q)
    dbconn.commit()
    print(index)
    return
try :
    dbconn = psycopg2.connect("dbname=linlib3test user=linlib password=linlib")
except :
    print("Cannot establish database connection")
    quit()
dbc = dbconn.cursor()
for line in fhandle :
    read_lines = read_lines + 1
    if len(line.strip()) > 0 :
        rec_len = rec_len + 1
        act_record.append(line)
    elif rec_len > 0 :
        read_records = read_records + 1
        last_record = act_record
        act_record = []
        to_save = rec_sorted(last_record)
        save_rec(to_save)
print(read_records)
#print(last_record)
#print(to_save)
#save_rec(to_save)
dbconn.close()
print("Connection closed")
fhandle.close()
