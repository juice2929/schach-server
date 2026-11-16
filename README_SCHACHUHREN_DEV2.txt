Schachuhren DEV2 - lokaler WebSocket-Testadapter

Voraussetzung: DEV1 bereits entpackt und die bisherigen Servermodule vorhanden.
Neue Dateien: online_tournament_ws_clock_dev.py, test_online_tournament_ws_clock_dev.py.
Keine bestehende Serverdatei ersetzen. Vor dem Start alten Testserver beenden.

Tests:
python -m unittest -v test_online_tournament_ws_clock_dev
Erwartet: 15 Tests (7 neue Uhr-/WebSocket-Tests, 8 bisherige Wiederverbindungstests).

Erst nach erfolgreichen Tests optional starten:
python online_tournament_ws_clock_dev.py
Bindet ausschliesslich 127.0.0.1:10003. Nicht gleichzeitig alten Server starten.
Alter Client erstellt weiterhin unbefristete Turniere; Zeitauswahl und Anzeige
werden im naechsten Entwicklungsschritt implementiert.

Protokoll: tournament_create akzeptiert optional time_minutes (0,2,3,5,10,15,30).
Zeitwerte aus Join-Nachrichten bestimmen die Konfiguration nicht.
Bei Zeitturnieren enthalten Snapshots time_minutes und Spielzustaende clock.
Private tournament_clock Nachrichten enthalten match_id, draw_replays und clock.
Der Server prueft Zeitablauf regelmaessig, auch ohne eingehenden Spielzug.
Lokaler Entwicklungsadapter ohne Persistenz oder Produktionshaertung.
Die geordnete Verarbeitung umfasst auch Socket-Sends; vor einem Internetbetrieb
muessen langsame Clients und Rueckstau gesondert behandelt und getestet werden.
