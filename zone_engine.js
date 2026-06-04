Last login: Thu Jun  4 22:29:38 on ttys054
davidalizadeh@MacBook-Air-von-David v2 % >....                                  
s = rd(p)
s = rep(p, s, u"Text('${widget.coachName} hat gerade keine Verf\u00fcgbarkeit.',", u"Text(l10n.dbNoAvailability(widget.coachName),")
wr(p, s)

p = u'lib/screens/ai_chat_screen.dart'
s = rd(p)
s = rep(p, s, u"'14,99 \u20ac/Mon. \u00b7 Jederzeit k\u00fcndbar',", u"l10n.aiPriceLine,")
wr(p, s)

# ---------------- Ergebnis ----------------
if ERR:
    print(u'FEHLER \u2014 NICHTS WEITER AUSFUEHREN:')
    for e in ERR:
        print(u'  - ' + e)
    sys.exit(1)
print(u'OK: Etappe 3 angewendet. 49 neue Keys (DE+EN), 9 Dart-Dateien gepatcht.')
PYEOF
python3 /tmp/l10n_e3.py
cd ~/Developer/Apps/biolyze.nosync/v2
flutter gen-l10n && flutter analyze lib/ 2>&1 | grep -c "error •"
OK: Etappe 3 angewendet. 49 neue Keys (DE+EN), 9 Dart-Dateien gepatcht.
Because l10n.yaml exists, the options defined there will be used instead.
To use the command line arguments, delete the l10n.yaml file in the Flutter
project.


0
davidalizadeh@MacBook-Air-von-David v2 % >....                                  
s = rep(p, s, u"""                          isDe
                              ? 'Auf Deutsch fortfahren'
                              : 'Auf Englisch fortfahren',""", u"""                          l10n.wsContinueButton,""")
s = rep(p, s, u"""                isDe
                    ? 'Du kannst die Sprache sp\u00e4ter in den Einstellungen \u00e4ndern'
                    : 'Du kannst die Sprache sp\u00e4ter in den Einstellungen \u00e4ndern',""", u"""                l10n.wsChangeLater,""")
wr(p, s)

# ---------------- Ergebnis ----------------
if ERR:
    print(u'FEHLER \u2014 NICHTS WEITER AUSFUEHREN:')
    for e in ERR:
        print(u'  - ' + e)
    sys.exit(1)
print(u'OK: Etappe 4 angewendet. 24 neue Keys (DE+EN), streak_card + welcome_screen gepatcht.')
PYEOF
python3 /tmp/l10n_e4.py
cd ~/Developer/Apps/biolyze.nosync/v2
flutter gen-l10n && flutter analyze lib/ 2>&1 | grep -c "error •"
OK: Etappe 4 angewendet. 24 neue Keys (DE+EN), streak_card + welcome_screen gepatcht.
Because l10n.yaml exists, the options defined there will be used instead.
To use the command line arguments, delete the l10n.yaml file in the Flutter
project.


0
davidalizadeh@MacBook-Air-von-David v2 % 
