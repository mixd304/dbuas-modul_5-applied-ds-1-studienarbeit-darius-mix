# dbuas-modul_5-applied-ds-1-studienarbeit-darius-mix

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