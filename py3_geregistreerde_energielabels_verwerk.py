# TODO: totaal bestand automatisch laten downloaden via een request naar de PublicAPI: https://public.ep-online.nl/swagger/index.html
# RVO: Bij een dubbel label geldt altijd dat de meest recente opnamedatum is het actuele label.

from psycopg2.extras import RealDictCursor
import psycopg2
import string
import os, sys
import configparser
from urllib.parse import urlparse
import csv
import zipfile
from pprint import pprint
import datetime

configfile = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
config = configparser.ConfigParser()
config.read(configfile)
uri=config.get('database','uri')
result = urlparse(uri)
username = result.username
password = result.password
database = result.path[1:]
hostname = result.hostname
port = result.port

if result.query == "sslmode=require":
    sslmode = 'require'
else:
    # default: https://ankane.org/postgres-sslmode-explained
    sslmode = 'prefer'
    
conn = psycopg2.connect(
    database = database,
    user = username,
    password = password,
    host = hostname,
    port = port,
    sslmode = sslmode
)

conn.autocommit = True
cur = conn.cursor(cursor_factory=RealDictCursor)

if len(sys.argv) == 2:
    filename = sys.argv[1]
    print(filename)
else:
    sys.exit()

f = open(filename, "r")
csvreader = csv.reader(f, delimiter=';')
header = next(csvreader)
# Pand_opnamedatum;Pand_opnametype;Pand_status;Pand_berekeningstype;Pand_energieindex;Pand_energieklasse;Pand_energielabel_is_prive;Pand_is_op_basis_van_referentie_gebouw;Pand_gebouwklasse;Meting_geldig_tot;Pand_registratiedatum;Pand_postcode;Pand_huisnummer;Pand_huisnummer_toev;Pand_detailaanduiding;Pand_bagverblijfsobjectid;Pand_bagligplaatsid;Pand_bagstandplaatsid;Pand_bagpandid;Pand_gebouwtype;Pand_gebouwsubtype;Pand_projectnaam;Pand_projectobject;Pand_SBIcode;Pand_gebruiksoppervlakte;Pand_energiebehoefte;Pand_eis_energiebehoefte;Pand_primaire_fossiele_energie;Pand_eis_primaire_fossiele_energie;Pand_primaire_fossiele_energie_EMG_forfaitair;Pand_aandeel_hernieuwbare_energie;Pand_eis_aandeel_hernieuwbare_energie;Pand_aandeel_hernieuwbare_energie_EMG_forfaitair;Pand_temperatuuroverschrijding;Pand_eis_temperatuuroverschrijding;Pand_warmtebehoefte;Pand_energieindex_met_EMG_forfaitair

h = {}
i = 0
for item in header:
    item = item.replace('"', '').strip()
    h[item] = i
    i += 1

o1 = open(filename[:-4] + "_energielabels_geregistreerd_vbo.csv", "w")
o1.write("Pand_energieklasse,Pand_bagverblijfsobjectid,Pand_opnamedatum\n")

o2 = open(filename[:-4] + "_energielabels_geregistreerd_ligplaats.csv", "w")
o2.write("Pand_energieklasse,Pand_bagligplaatsid,Pand_opnamedatum\n")

o3 = open(filename[:-4] + "_energielabels_geregistreerd_standplaats.csv", "w")
o3.write("Pand_energieklasse,Pand_bagstandplaatsid,Pand_opnamedatum\n")

o4 = open(filename[:-4] + "_energielabels_geregistreerd_pch.csv", "w")
o4.write("Pand_energieklasse,Pand_postcode,Pand_huisnummer,Pand_huisnummer_toev,Pand_opnamedatum\n")

a = 0
b = 0
c = 0
d = 0
e = 0
f = 0
g = 0
totaal = 0
dubbel = 0

vbo_ids = {}
standplaats_ids = {}
ligplaats_ids = {}

check = {}
# s = {}
for line in csvreader:
    Pand_energieklasse = line[h["Pand_energieklasse"]].strip()
    Pand_gebouwklasse = line[h["Pand_gebouwklasse"]].strip()
    Pand_status = line[h["Pand_status"]].strip()
    if Pand_status == 'Vergunningsaanvraag':
        continue
    Pand_opnamedatum = line[h["Pand_opnamedatum"]].strip()
    year = int(Pand_opnamedatum[:4])
    month = int(Pand_opnamedatum[4:6])
    day = int(Pand_opnamedatum[6:])
    Pand_opnamedatum = datetime.date(year, month, day)
    if Pand_energieklasse != "":
        Pand_bagverblijfsobjectid = line[h["Pand_bagverblijfsobjectid"]].strip()
        Pand_bagligplaatsid = line[h["Pand_bagligplaatsid"]].strip()
        Pand_bagstandplaatsid = line[h["Pand_bagstandplaatsid"]].strip()
        if Pand_bagverblijfsobjectid != "":
            # vbo
            if Pand_bagverblijfsobjectid in vbo_ids.keys():
                dubbel += 1
                if Pand_opnamedatum > vbo_ids[Pand_bagverblijfsobjectid][1]:
                    vbo_ids[Pand_bagverblijfsobjectid] = [Pand_energieklasse,Pand_opnamedatum]
            else:
                vbo_ids[Pand_bagverblijfsobjectid] = [Pand_energieklasse,Pand_opnamedatum]
            a += 1
        elif Pand_bagligplaatsid != "":
            # ligplaats
            if Pand_bagligplaatsid in ligplaats_ids.keys():
                dubbel += 1
                if Pand_opnamedatum > ligplaats_ids[Pand_bagligplaatsid][1]:
                    ligplaats_ids[Pand_bagligplaatsid] = [Pand_energieklasse,Pand_opnamedatum]
            else:
                ligplaats_ids[Pand_bagligplaatsid] = [Pand_energieklasse,Pand_opnamedatum]
            b += 1
        elif Pand_bagstandplaatsid != "":
            # standplaats
            if Pand_bagstandplaatsid in standplaats_ids.keys():
                dubbel += 1
                if Pand_opnamedatum > standplaats_ids[Pand_bagstandplaatsid][1]:
                    standplaats_ids[Pand_bagstandplaatsid] = [Pand_energieklasse,Pand_opnamedatum]
            else:
                standplaats_ids[Pand_bagstandplaatsid] = [Pand_energieklasse,Pand_opnamedatum]
            c += 1
        else:
            # pch
            Pand_postcode = line[h["Pand_postcode"]].strip()
            Pand_huisnummer = line[h["Pand_huisnummer"]].strip()
            Pand_huisnummer_toev = line[h["Pand_huisnummer_toev"]].strip()

            if "%s%s%s" % (Pand_postcode,Pand_huisnummer,Pand_huisnummer_toev) != "":
                sql_string = '''select adresseerbaarobject_id, typeadresseerbaarobject from bag.adres_plus_energielabel where pchn_compare = '%s %s%s';''' % (Pand_postcode.upper(), Pand_huisnummer.upper(), Pand_huisnummer_toev.upper())
                cur.execute(sql_string)
                row = cur.fetchone()
                if row != None:
                    if row['typeadresseerbaarobject'] == 'Verblijfsobject':
                        Pand_bagverblijfsobjectid = row['adresseerbaarobject_id']
                        if Pand_bagverblijfsobjectid in vbo_ids.keys():
                            dubbel += 1
                            if Pand_opnamedatum > vbo_ids[Pand_bagverblijfsobjectid][1]:
                                vbo_ids[Pand_bagverblijfsobjectid] = [Pand_energieklasse,Pand_opnamedatum]
                        else:
                            vbo_ids[Pand_bagverblijfsobjectid] = [Pand_energieklasse,Pand_opnamedatum]
                        a += 1
                    elif row['typeadresseerbaarobject'] == 'Standplaats':
                        Pand_bagstandplaatsid = row['adresseerbaarobject_id']
                        if Pand_bagstandplaatsid in standplaats_ids.keys():
                            dubbel += 1
                            if Pand_opnamedatum > standplaats_ids[Pand_bagstandplaatsid][1]:
                                standplaats_ids[Pand_bagstandplaatsid] = [Pand_energieklasse,Pand_opnamedatum]
                        else:
                            standplaats_ids[Pand_bagstandplaatsid] = [Pand_energieklasse,Pand_opnamedatum]
                        b += 1
                    elif row['typeadresseerbaarobject'] == 'Ligplaats':
                        Pand_bagligplaatsid = row['adresseerbaarobject_id']
                        if Pand_bagligplaatsid in ligplaats_ids.keys():
                            dubbel += 1
                            if Pand_opnamedatum > ligplaats_ids[Pand_bagligplaatsid][1]:
                                ligplaats_ids[Pand_bagligplaatsid] = [Pand_energieklasse,Pand_opnamedatum]
                        else:
                            ligplaats_ids[Pand_bagligplaatsid] = [Pand_energieklasse,Pand_opnamedatum]
                        c += 1
                    else:
                        print(row['typeadresseerbaarobject'])
                    d += 1
                else:
                    # wel adres, maar lukt niet te koppelen aan BAG
                    o4.write("%s,%s,%s,%s,%s\n" % (Pand_energieklasse,Pand_postcode,Pand_huisnummer,Pand_huisnummer_toev,Pand_opnamedatum))
                    e += 1
            else:
                # geen adres
                f += 1
    else:
        # geen energielabel
        g += 1
    totaal += 1

for Pand_bagverblijfsobjectid in vbo_ids.keys():
    [Pand_energieklasse,Pand_opnamedatum] = vbo_ids[Pand_bagverblijfsobjectid]
    o1.write("%s,%s,%s\n" %(Pand_energieklasse,Pand_bagverblijfsobjectid,Pand_opnamedatum))

for Pand_bagligplaatsid in ligplaats_ids.keys():
    [Pand_energieklasse,Pand_opnamedatum] = ligplaats_ids[Pand_bagligplaatsid]
    o2.write("%s,%s,%s\n" %(Pand_energieklasse,Pand_bagligplaatsid,Pand_opnamedatum))
    
for Pand_bagstandplaatsid in standplaats_ids.keys():
    [Pand_energieklasse,Pand_opnamedatum] = standplaats_ids[Pand_bagstandplaatsid]
    o3.write("%s,%s,%s\n" %(Pand_energieklasse,Pand_bagstandplaatsid,Pand_opnamedatum))

o1.close()
o2.close()
o3.close()
o4.close()

result = {
'aantal vbo id':  a,
'aantal ligplaats id':  b,
'aantal standplaats id':  c,
'aantal alleen pch (kunnen koppelen aan BAG)':  d,
'aantal alleen pch (niet gekoppeld aan BAG)':  e,
'met energielabel, zonder locatie': f,
'rest zonder energielabel':  g,
'dubbelingen': dubbel,
'totaal': totaal
}
pprint(result)
