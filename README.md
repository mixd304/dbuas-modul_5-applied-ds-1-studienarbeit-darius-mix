# dbuas-modul_5-applied-ds-1-studienarbeit-darius-mix

## Github-Link

Das Repository ist auch über den folgenden Github Link abrufbar: https://github.com/mixd304/dbuas-modul_5-applied-ds-1-studienarbeit-darius-mix.

## Informationen zu den Ordnern und Dateien in der Arbeit
```requirements.txt``` - enthält die benötigten Packages zum Ausführen des Python Codes (wird im Setup verwendet).  <br>
Ordner ```input``` - enthält die web-sources (Verwendet für den Scraper) und die E-Sports Events CSV.  <br>
Ordner ```output``` - enthält jeglichen Output des Python Codes, u.a. das Data-Lake (alle HTML-Dateien) und das DWH, also die SQLite-Datei.  <br>
Ordner ```lib_py``` - enthält jegliche Python Skript-Dateien, die für die Datenerhebung, Speicherung und Analyse benötigt werden.  <br>
Ordner ```notebooks``` - enthält das Notebook, mit welchem die Datenanalyse durchgeführt wurde.  <br>
Ordner ```logs``` - enthält die Logs der ```eda.py```.  <br>

## Setup

### Install packages

#### pipenv
```
$ pipenv install -r requirements.txt
```

#### conda
```
$ conda init
$ conda create --name dbuas-ads-01-studienarbeit-dariusmix --file requirements.txt
$ conda info --envs
$ conda activate dbuas-ads-01-studienarbeit-dariusmix
```

zum entfernen der neuen Umgebung aus conda:
```
$ conda env remove --name dbuas-ads-01-studienarbeit-dariusmix
```

Hinweis:
Sollte es beim anlegen der neuen Umgebung in conda zu einem Fehler kommen, sollten die Befehle direkt über die Anaconda prompt ausgeführt werden.

## Execute
```
$ python lib_py/scraper.py
$ python lib_py/etl.py
```