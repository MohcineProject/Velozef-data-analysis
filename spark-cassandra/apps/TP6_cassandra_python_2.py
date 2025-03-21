from cassandra.cluster import Cluster
import pandas as pd 
clstr=Cluster(['172.22.0.1'])

session=clstr.connect()
# session.execute("create keyspace mykeyspace with replication={'class': 'SimpleStrategy', 'replication_factor' : 3};")

# qry= '''
# # create table mykeyspace.etudiants (EtudiantID int, Nom text, Age int, Note int, primary key(EtudiantID));'''

# session.execute(qry)
rows=session.execute("select * from mykeyspace.etudiants;")

# session.execute("delete from mykeyspace.etudiants where EtudiantID=2")
for row in rows:
    print('EtudiantID: {} Nom:{} Age:{} Note:{}'.format(row[0],row[1], row[2], row[3]))
data = {
    'EtudiantID':[2,3,4],
    'Nom' : ['Smith','Dupond','Reuter'],
    'Age' : [19,21,21],
    'Note':[10,12,14]
}
df = pd.DataFrame(data)
print("#########################################")
# INSERT_QUERY = """
# INSERT INTO mykeyspace.etudiants (EtudiantID,Nom,Age,Note) VALUES (%s %s %s %s)
# """
for index, row in df.iterrows():
    session.execute(f"insert into mykeyspace.etudiants (EtudiantID, Nom, Age, Note) values ({row['EtudiantID']}, '{row['Nom']}', {row['Age']}, {row['Note']})")

rows=session.execute("select * from mykeyspace.etudiants;")

for row in rows:
    print('EtudiantID: {} Nom:{} Age:{} Note:{}'.format(row[0],row[1], row[2], row[3]))

# rows=session.execute("select * from mykeyspace.etudiants WHERE age>20 allow filtering;")

rows = session.execute("SELECT * FROM mykeyspace.etudiants WHERE Age = 21 ALLOW FILTERING")

best_note=0
for row in rows : 
    if row.note > best_note:
        best_note = row.note
        best_student = row.nom

print(f"Le meilleur étudiant est {best_student} avec une note de {best_note}")