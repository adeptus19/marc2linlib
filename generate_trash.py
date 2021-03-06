import psycopg2
import configparser
config = configparser.ConfigParser()
config.read('config.ini')
dbname = config["DATABASE"]["dbname"]
dbuser = config["DATABASE"]["dbuser"]
dbpassword = config["DATABASE"]["dbpassword"]
conn_s = "dbname=" + dbname + " user=" + dbuser +"  password=" +dbpassword
try :
    dbconn = psycopg2.connect(conn_s)
except :
    print("Cannot establish database connection")
    quit()
dbc = dbconn.cursor()
whichfile = input("Selejtjegyzék sorszáma: ")
file_to_process = "./selejt/selejt" + whichfile + ".txt"
s1 = input("Selejtjegyzék azonosítója: ")
sqlfile = "./selejt/trashgen" + s1.replace("/", "_") + ".sql"
try :
    fhandle = open(file_to_process, "r")
except:
    print("Error by opening file")
    quit()
sqlhandle = open(sqlfile, "w", encoding="latin2")
read_lines = 0
query1 ="SELECT COUNT(*) FROM selejtj;"
dbc.execute(query1)
res = dbc.fetchone()
index = 1
sqlcommand = "INSERT INTO selejtj (sorszam, jegyzek, ar, lszam, szerzo, cim, szakj, raktj, pnem, lbetu, lhely, kotszam, db, torles, doktipus, kpld) VALUES ("
for id in fhandle:
    id = id.rstrip()
    values = ""
    read_lines = read_lines + 1
    search  = "SELECT kpld.sorszam, konyv.id, ar, lszam, szerzo,fc,szakj,raktj,pnem,lbetu, lhely,kotszam FROM konyv INNER JOIN kpld ON konyv.id = kpld.id WHERE vonalkod = '" + id +" ';"
    dbc.execute(search)
    res=dbc.fetchone()
    if (not (res is None)):
        values = str(index) +", '" + s1 + "', "
        print(read_lines)
        if (float(res[2])> 0):
            values = values + str(res[2]) + ", "
        else:
            values = values + "0, "
        values = values + str(res[3]) + ", "
        for i in range(4,len(res) - 1 ):
            if (res[i] != ""):
                unescaped = res[i].replace("'", "\''")
                if (i==4):
                    unescaped = unescaped[:30]
                if (i==5):
                    unescaped = unescaped[:40]
                if (i==10):
                    unescaped = unescaped[:15]
                values = values + "'" + unescaped + "', "
            else:
                values = values + "' ', "
        if ((res[11] != "") and (res[11] is not None)):
            values = values + "'" + str(res[11]) + "', "
        else:
            values = values + "' ', "
        values = values + "1, " + "'Elavulás', " + "'Könyv', " + str(res[0]) +");"
        list = sqlcommand + values + "\n"
        print(list)
        sqlhandle.write(list)
        index = index + 1
fhandle.close
sqlhandle.close()
dbc.close()