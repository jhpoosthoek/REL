import os, sys
import csv
import zipfile

if len(sys.argv) == 2:
    filename = sys.argv[1]
    print(filename)
else:
    sys.exit()

zf = zipfile.ZipFile(filename)
f = zf.read(filename.replace('zip', 'csv'))
csvreader = csv.reader(csv.StringIO(f.decode()), delimiter=';')
header = next(csvreader)
# Pand_opnamedatum;Pand_opnametype;Pand_status;Pand_berekeningstype;Pand_energieindex;Pand_energieklasse;Pand_energielabel_is_prive;Pand_is_op_basis_van_referentie_gebouw;Pand_gebouwklasse;Meting_geldig_tot;Pand_registratiedatum;Pand_postcode;Pand_huisnummer;Pand_huisnummer_toev;Pand_detailaanduiding;Pand_bagverblijfsobjectid;Pand_bagligplaatsid;Pand_bagstandplaatsid;Pand_bagpandid;Pand_gebouwtype;Pand_gebouwsubtype;Pand_projectnaam;Pand_projectobject;Pand_SBIcode;Pand_gebruiksoppervlakte;Pand_energiebehoefte;Pand_eis_energiebehoefte;Pand_primaire_fossiele_energie;Pand_eis_primaire_fossiele_energie;Pand_primaire_fossiele_energie_EMG_forfaitair;Pand_aandeel_hernieuwbare_energie;Pand_eis_aandeel_hernieuwbare_energie;Pand_aandeel_hernieuwbare_energie_EMG_forfaitair;Pand_temperatuuroverschrijding;Pand_eis_temperatuuroverschrijding;Pand_warmtebehoefte;Pand_energieindex_met_EMG_forfaitair

h = {}
i = 0
for item in header:
    item = item.replace('"', '').strip()
    h[item] = i
    i += 1

o1 = open(filename[:-4] + "_woningen.csv", "w")
o1.write(";".join(header) + "\n")

o2 = open(filename[:-4] + "_utiliteit.csv", "w")
o2.write(";".join(header))

for line in csvreader:
    Pand_gebouwklasse = line[h["Pand_gebouwklasse"]].strip()
    if Pand_gebouwklasse == 'W':
        o1.write(";".join(line) + "\n")
    else:
        o2.write(";".join(line) + "\n")

o1.close()
o2.close()
