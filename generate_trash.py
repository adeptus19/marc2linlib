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
file_to_process = "./selejt/selejt1.txt"
sqlfile = "./selejt/trashgen.sql"
try :
    fhandle = open(file_to_process, "r", encoding = "latin2")
except:
    print("Error by opening file")
    quit()
sqlhandle = open(sqlfile, "w")
read_lines = 0;
s1 = "2021/01"
index = 1
sqlcommand = "INSERT INTO selejtj (sorszam, jegyzek, ar, pnem, szerzo, fc, szakj, raktj, lbetu, lszam, lhely, kotszam, db, torles, doktipus, kpld) VALUES ("
for id in fhandle:
    id = id.rstrip()
    values = ""
    read_lines = read_lines + 1
    search  = "SELECT konyv.id, ar, konyv.doktipus,kpld.sorszam,szerzo,fc,szakj,raktj,pnem,lbetu,lszam,lhely,kotszam FROM konyv INNER JOIN kpld ON konyv.id = kpld.id WHERE vonalkod = '" + id +" ';"
    dbc.execute(search)
    res=dbc.fetchone()
    if (not (res is None)):
        values = str(index) +", '" + s1 + "', "
        print(read_lines)
        if (float(res[1])> 0):
            values = values + str(res[1]) + ", "
        else:
            values = values + "0, "
        for i in range(2,len(res) - 1 ):
            if (res[i] != ""):
                values = values + "'" + str(res[i]) + "', "
            else:
                values = values + "' ', "
        print(values)
        if ((res[12] != "") and (res[12] is not None)):
            values = values + "'" + str(res[13]) + "', "
        else:
            values = values + "' ', "
        index = index + 1
fhandle.close
sqlhandle.close()
dbc.close()