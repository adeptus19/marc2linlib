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
querycount = "SELECT count(*) from konyv"
dbc.execute(querycount)
konyvdbs = dbc.fetchone()
konyvdb = int(konyvdbs[0])
for konyv_id in range(1, konyvdb):
    select1 = "SELECT doktipus, szerzo, fc, szakj, raktj, ar, pnem FROM konyv WHERE id = " + str(konyv_id) + ";"
    dbc.execute(select1)
    res1 = dbc.fetchone()
    select2 = "SELECT lszam FROM kpld WHERE id = " + str(konyv_id) + ";"
    dbc.execute(select2)
    res2=dbc.fetchall()
    ins_val = ""
    #sample insert:
    # INSERT INTO lkonyv (doktipus, szerzo, cim, szakj, raktj, ar, pnem, lszam, statusz)
    #   VALUES ('Könyv', 'Jókai', 'Arany ember', 'J23', '7555', 2000.0,, 'Ft', '3627627', 1);
    for i in range(len(res1)):
        if i == 5 : 
            ins_val = ins_val + str(res1[i]) +", "
        elif i == 2:
            ins_val = ins_val + "'" + str(res1[i])[:50].replace("\'","''") + "', " 
        else:
            ins_val = ins_val + "'" + str(res1[i]).replace("\'","''") + "', " 
    print(ins_val)
    for pldcount in range(len(res2)) :
        ins_val_secpart = ""
        for i in range(len(res2[pldcount])):
            ins_val_secpart = ins_val_secpart + str(res2[pldcount][i]) + ", "
        ins_val_secpart = ins_val_secpart.rstrip(", ")
        ins_full = "INSERT INTO lkonyv (doktipus, szerzo, cim, szakj, raktj, ar, pnem, lszam, statusz) VALUES ("+ ins_val + ins_val_secpart + ", 1);"
        dbc.execute(ins_full)
dbconn.commit()
dbconn.close()