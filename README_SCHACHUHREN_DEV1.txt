Schachspiel Ultimate Online V1.3 - Schachuhren, Entwicklungsstufe 1

Vier neue Dateien nur in Schachserver_Turnier_Dev entpacken.
Vorhandene Dateien, insbesondere server.py und online_tournament_resume_dev.py,
werden nicht ersetzt. Diese Module starten keinen Server. GUI und WebSocket-
Integration folgen separat. Die bestehende Startdatei kennt diese Uhren noch nicht.

Tests (PowerShell im Server-Entwicklungsordner):
python -m unittest -v test_online_tournament_clock_dev test_online_tournament_timed_games_dev
Optional anschliessend: python -m unittest discover -v

Zeitoptionen: 0 (ohne Uhr), 2,3,5,10,15,30 Minuten pro Spieler, ohne Inkrement.
Uhr beginnt fuer Weiss, sobald eine Paarung spielbereit ist. Sie laeuft bei
Verbindungsabbruch weiter. Remiswiederholung startet mit voller Zeit.
Zeitablauf wird im Spielmodul bei Aktionen und beim periodischen poll_expired()
geprueft; ein zukuenftiger WebSocket-Adapter muss dieses Polling ausfuehren,
unter derselben Sperre wie Zuege. Kein Netzwerkbetrieb in diesem Paket.
Bei Zeitablauf und erkanntem unzureichenden Mattmaterial des Gegners: Remis
und Wiederholung. python-chess prueft Material, nicht jede denkbare tote Stellung.

Geprueft in der Erstellungsumgebung: 17 Uhr-/Buchtests und 15 alte Kern-Tests;
Syntax aller neuen Python-Dateien. Die 11 Spielintegrationstests benoetigen
python-chess und muessen auf dem Windows-PC noch ausgefuehrt werden.
