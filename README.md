# Abgabe_Sturm

## Daten
  Der Datensatz, der zur Erzeugung des Streams genutzt wird ist: https://www.kaggle.com/datasets/yasserh/customer-segmentation-dataset  
  Da die Daten hier als xlsx-Datei angeboten werden, beinhaltet das Repo auch das genutzte Script um die Daten in eine csv-Datei umzuwandeln (abgabe/xlsx_to_csv.py).

## Business Case

  Die Daten enthalten die Verkaufsdaten eines Online-Händlers, der zum größten Teil im Vereinigten Königreich agiert.
  Der Online-Händler möchte die Verkaufsdaten der Kunden aus dem Vereinigten Königreich untersuchen.
  Mit Hilfe des Clusterings sollen mögliche Muster bzw. Gruppierungen in den Kundendaten gefunden werden.
  Der Stream an Verkaufsdaten, wird nach Vereinigtem Königreich und anderen Ländern aufgeteilt und auf die Daten aus dem Vereinigten Königreich wird ein Clustering-Algorithmus angewendet (StreamingKMeans).  
  Die Daten aus dem Vereinigten Königreich werden mit der Vorhersage in eine Datenbanktabelle geschrieben. Die Daten aus den anderen Ländern werden in einer separaten Tabelle gespeichert.

## Componenten

### Stream Erzeuger

  Ein Pythonscript (abgabe/streaming_server.py) erzeugt den Datenstrom, aus einer im Repo enthaltenen csv Datei (data/Online_Retail.csv).

### Data Ingestion Layer

  Kafka, in einem Docker Container, übernimmt die Aufgabe des Data Ingestion Layer und nimmt den Datenstrom in eine Message Queue auf.

### Stream Processing Layer

  Ein Pythonscript (abgabe/stream_data_processor.py) verarbeitet, mit Hilfe von pyspark, die Daten aus der Message Queue und speichert diese in einer MariaDB.

### Serving Layer

  Eine MariaDB innerhalb eines Docker Containers dient als Serving Layer. Dort können die Daten abgerufen werden.

## Ausführung

  Funktionsfähigkeit des Projekts nur auf einer Linux-Maschine gewährleistet.

1. Clonen des Repos. 
2. Öffnen einer Konsole und navigieren zum Unterordner Abgabe_Sturm/abgabe
3. Erzeugen des Docker Containers mit:   
    "docker-compose up -d"  
   Zur Vorbereitung des nächsten Schrittes, in den vorherigen Ordner zurückkehren mit:   
   "cd .."
3. Erstellen und aktivieren eines venv mit:   
    "python3 -m venv venv"  
    "source venv/bin/activate"
4. Installieren von dependencies mit:  
    "python3 -m pip install -r requirements.txt"
5. Starten des Datastreaming mit:  
    "python abgabe/streaming_server.py"
7. Öffnen einer neuen Konsole im Ordner Abgabe_Sturm. Aktivieren des venv in dieser Konsole mit:  
    "source venv/bin/activate"
8. Starten der Stream Processing Layer mit:
    "python abgabe/stream_data_processor.py"  

Nun sollten alle Komponenten aktiv sein.
- der Docker Container der Kafka und MariaDB enthält
- eine Konsole in der streaming_server.py den Datenstrom erzeugt
- eine Konsole in der stream_data_processor.py die Daten verarbeitet und in die DB speichert

## Abrufen der Ergebnisse

Unter <http://localhost:8080/> ist die MariaDB erreichbar. Login mittels:

Username: root 
Passwort: example
Database: kappa-view  

In der DB befinden sich zwei Tabellen:
- **British_Online_Retail:** diese enthält die Daten, die aus dem Vereinigten Königreich stammen und geclusterted wurden.
- **Other_Online_Retail:** enthält die Daten aus den anderen Ländern.

## Stoppen der Anwendung

- Unterbrechen der laufenden Scripte
- Stoppen des Docker Containers (docker-compose down)
