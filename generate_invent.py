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
for konyv_id in range(1, konyvdb+1):
    select1 = "SELECT doktipus, szerzo, fc, szakj, raktj, ar, pnem FROM konyv WHERE id = " + str(konyv_id) + ";"
    dbc.execute(select1)
    res1 = dbc.fetchone()
    select2 = "SELECT lszam, lbetu FROM kpld WHERE id = " + str(konyv_id) + ";"
    dbc.execute(select2)
    res2=dbc.fetchall()
    ins_val = ""
    # INSERT INTO lkonyv (doktipus, szerzo, cim, szakj, raktj, ar, pnem, lszam, statusz, megan)
    #sample insert:
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
            if str(res2[pldcount][i]) == '':
                ins_val_secpart = ins_val_secpart + "\' \'" + ", "
            else:
                if i == 0:
                    ins_val_secpart = ins_val_secpart + str(res2[pldcount][i]) + ", "
                else:
                    ins_val_secpart = ins_val_secpart + "\'" + str(res2[pldcount][i]) + "\', "
        ins_val_secpart = ins_val_secpart.rstrip(", ")
        print(ins_val_secpart)
        ins_full = "INSERT INTO lkonyv (doktipus, szerzo, cim, szakj, raktj, ar, pnem, lszam, lbetu, statusz) VALUES ("+ ins_val + ins_val_secpart + ", 1);"
        print(ins_full)
        dbc.execute(ins_full)
        dbconn.commit()
dbconn.close()