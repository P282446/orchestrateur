#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lanceur local : UN SEUL fichier à conserver sur votre machine.

À chaque exécution, il :
  1. se connecte à Grid'5000 (SSH, via la passerelle access.grid5000.fr) ;
  2. clone vos dépôts GitHub (orchestrateur + les 5 pipelines) dans un dossier daté
     (~/pipelines/<date-heure>/) : les emplacements ne dépendent plus d'aucune machine ;
  3. envoie les vidéos et les fichiers de fixations des entretiens (rsync) ;
  4. soumet les entretiens (un job de 12 h chacun, enchaînés : voir orchestrateur.py) ;
  5. ramène les résultats dans le dossier local de votre choix.

Chaque lot de pipelines est conservé : les entretiens déjà soumis continuent avec la
version du code qui était sur GitHub au moment du lancement, même si vous poussez
ensuite des modifications. Les versions (commits) utilisées sont enregistrées dans
resultats/versions_pipelines.txt de chaque entretien.

Commandes :
    python3 lancer_entretiens.py verifier
    python3 lancer_entretiens.py lancer --manifeste entretiens.csv [--attendre]
    python3 lancer_entretiens.py lancer --entretien BOC.066 --video-sujet V_S.wmv \\
        --video-psy V_P.wmv --fix-sujet F_S.txt --fix-psy F_P.txt [--attendre]
    python3 lancer_entretiens.py statut
    python3 lancer_entretiens.py recuperer [ENTRETIEN ...] [--sortie DOSSIER] [--avec-logs]
    python3 lancer_entretiens.py journal ENTRETIEN [--unite segmentation_sujet] [--lignes 40]
    python3 lancer_entretiens.py arbre [--profondeur 3]
    python3 lancer_entretiens.py relancer [ENTRETIEN ...]

Manifeste (CSV) : entretien, video_sujet, video_psy, fix_sujet, fix_psy
(chemins locaux, relatifs au manifeste ou absolus).

Réglages (login, dépôts GitHub...) : fichier lanceur.ini à côté de ce script.

Prérequis sur votre machine : Python 3.8+, ssh et rsync (Linux/macOS ou WSL).
Prérequis une seule fois sur Grid'5000 : voir le README (environnement Python,
jeton Hugging Face, modèles) ; la connexion SSH par clé vers Grid'5000 doit marcher
et, pour des dépôts GitHub privés, votre agent SSH est transmis (option -A).

Dépendances Python : bibliothèque standard uniquement.
"""

import argparse
import csv
import io
import json
import os
import re
import secrets
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# ==============================================================================
# CONFIGURATION - valeurs par défaut. Vos réglages personnels vont dans lanceur.ini (à côté de
# ce fichier, modèle : lanceur_exemple.ini) : ils remplacent ceux-ci et survivent aux mises à jour.
# ==============================================================================
G5K_LOGIN = "smbengue"
G5K_SITE = "nancy"
# Alias de votre ~/.ssh/config (ex. "nancy.g5k") ; None = connexion directe via
# access.grid5000.fr avec G5K_LOGIN / G5K_SITE (équivalent de :
# ssh -J LOGIN@access.grid5000.fr LOGIN@SITE.grid5000.fr). "local" = mode essai sans Grid'5000.
SSH_HOTE = None

DOSSIER_RESULTATS = "~/resultats_entretiens"      # sur VOTRE machine
DOSSIER_ENTREES_DISTANT = "~/entretiens/entrees"  # sur Grid'5000 : entrées envoyées
DOSSIER_LOTS_DISTANT = "~/pipelines"              # sur Grid'5000 : un sous-dossier par lancement

# Dépôts GitHub : (nom du dossier de clonage, URL, branche / tag / commit).
# Les noms doivent correspondre aux {pipelines}/<nom>/... de config.ini.
DEPOTS = [
    ("orchestration",     "https://github.com/VOTRE_COMPTE/pipelines-entretiens.git", "main"),
    ("segmentation",     "https://github.com/VOTRE_COMPTE/segmentation.git",         "main"),
    ("synchronisation",   "https://github.com/VOTRE_COMPTE/synchronisation.git",      "main"),
    ("mesures_manquantes", "https://github.com/VOTRE_COMPTE/mesures_manquantes.git",  "main"),
    ("transcription",     "https://github.com/VOTRE_COMPTE/transcription.git",        "main"),
    ("analyse",           "https://github.com/VOTRE_COMPTE/analyse.git",              "main"),
]
DEPOT_ORCHESTRATION = "orchestration"   # dépôt qui contient les deux fichiers suivants
FICHIER_ORCHESTRATEUR = "orchestrateur.py"
FICHIER_CONFIG = "config.ini"

INTERVALLE_ATTENTE_MIN = 10   # --attendre : fréquence de vérification
RELANCES_AUTO = 1             # --attendre : relances d'un entretien interrompu (fin des 12 h)
# ==============================================================================

def charger_reglages():
    """Réglages personnels : fichier lanceur.ini placé à côté de ce script (ou indiqué par
    la variable d'environnement LANCEUR_CONFIG). Ils remplacent les valeurs ci-dessus, ce qui
    permet de remplacer ce fichier par une nouvelle version sans perdre ses réglages."""
    import configparser
    chemin = Path(os.environ.get("LANCEUR_CONFIG") or Path(__file__).resolve().with_name("lanceur.ini"))
    if not chemin.is_file():
        return None
    cp = configparser.ConfigParser(interpolation=None)
    cp.optionxform = str
    # espaces insécables / invisibles (copier-coller depuis une page web) -> espaces normaux
    texte = chemin.read_text(encoding="utf-8-sig").replace("\u00a0", " ").replace("\u200b", "")
    cp.read_string(texte)
    g = globals()
    textes = {"g5k_login": "G5K_LOGIN", "g5k_site": "G5K_SITE", "ssh_hote": "SSH_HOTE",
              "dossier_resultats": "DOSSIER_RESULTATS", "dossier_entrees_distant": "DOSSIER_ENTREES_DISTANT",
              "dossier_lots_distant": "DOSSIER_LOTS_DISTANT", "depot_orchestration": "DEPOT_ORCHESTRATION",
              "fichier_orchestrateur": "FICHIER_ORCHESTRATEUR", "fichier_config": "FICHIER_CONFIG"}
    entiers = {"intervalle_attente_min": "INTERVALLE_ATTENTE_MIN", "relances_auto": "RELANCES_AUTO"}
    if cp.has_section("lanceur"):
        for cle, val in cp["lanceur"].items():
            val = val.strip()
            if cle in textes:
                g[textes[cle]] = (val or None) if cle == "ssh_hote" else val
            elif cle in entiers:
                g[entiers[cle]] = int(val)
            else:
                sys.exit(f"{chemin.name} : réglage inconnu '{cle}'")
    if cp.has_section("depots"):
        depots = []
        for nom, val in cp["depots"].items():
            morceaux = val.split()
            if not morceaux:
                sys.exit(f"{chemin.name} : URL manquante pour le dépôt '{nom}'")
            depots.append((nom.strip(), morceaux[0], morceaux[1] if len(morceaux) > 1 else "main"))
        DEPOTS[:] = depots
    return chemin


CLES = ["video_sujet", "video_psy", "fix_sujet", "fix_psy"]
q = shlex.quote


def nettoyer(nom):
    return re.sub(r"[^\w.\-]+", "_", nom).strip("_") or "x"


# ------------------------------------------------------------------------------
# Connexion à l'hôte (Grid'5000 par SSH, ou mode essai local)
# ------------------------------------------------------------------------------
class Hote:
    def __init__(self, ssh_hote):
        self.local = ssh_hote == "local"
        if self.local:
            return
        ctl = os.path.join(tempfile.gettempdir(), f"g5k-ssh-{os.getuid()}-%C")
        self.ssh = ["ssh", "-A", "-o", "ControlMaster=auto", "-o", f"ControlPath={ctl}",
                    "-o", "ControlPersist=15m", "-o", "ServerAliveInterval=30"]
        if ssh_hote:
            self.cible = ssh_hote
        else:
            if G5K_LOGIN.startswith("votre_"):
                sys.exit("Renseignez G5K_LOGIN (votre identifiant Grid'5000) en haut de ce fichier, "
                         "ou utilisez un alias SSH (SSH_HOTE = \"nancy.g5k\").")
            self.ssh += ["-J", f"{G5K_LOGIN}@access.grid5000.fr"]
            self.cible = f"{G5K_LOGIN}@{G5K_SITE}.grid5000.fr"

    def executer(self, script, capture=False, check=True):
        cmd = ["bash", "-s"] if self.local else self.ssh + [self.cible, "bash -s"]
        r = subprocess.run(cmd, input="set -euo pipefail\n" + script, text=True,
                           capture_output=capture)
        if check and r.returncode == 255 and not self.local:
            test = " ".join(q(x) for x in self.ssh + [self.cible, "hostname"])
            sys.exit("Connexion SSH à Grid'5000 impossible (code 255).\n"
                     f"Testez à la main :  {test}\n"
                     "Pistes : identifiant G5K_LOGIN incorrect ; clé SSH non enregistrée sur Grid'5000 "
                     "ou non chargée (ssh-add) ; ou utilisez un alias de ~/.ssh/config "
                     "(SSH_HOTE = \"nancy.g5k\", voir le README).")
        if check and r.returncode != 0:
            detail = (r.stderr or "").strip() if capture else ""
            sys.exit(f"Commande distante en échec (code {r.returncode}). {detail}".strip())
        return r

    def home(self):
        sortie = self.executer('echo "$HOME"', capture=True).stdout.strip().splitlines()
        return sortie[-1]

    def _rsync(self, src, dest, distant_src=False):
        if shutil.which("rsync"):
            cmd = ["rsync", "-a", "--partial", "--info=progress2"]
            if not self.local:
                cmd += ["-e", " ".join(q(x) for x in self.ssh)]
        elif self.local:
            cmd = ["cp", "-a"]
            src, dest = src.rstrip("/") + "/." if src.endswith("/") else src, dest
        else:
            sys.exit("rsync est requis sur votre machine.")
        if not self.local:
            if distant_src:
                src = f"{self.cible}:{src}"
            else:
                dest = f"{self.cible}:{dest}"
        subprocess.run(cmd + [src, dest], check=True)

    def envoyer(self, src_local, dest_distant):
        self.executer("mkdir -p " + q(os.path.dirname(dest_distant)))
        self._rsync(src_local, dest_distant)

    def telecharger(self, dossier_distant, dossier_local):
        Path(dossier_local).mkdir(parents=True, exist_ok=True)
        self._rsync(dossier_distant.rstrip("/") + "/", str(dossier_local).rstrip("/") + "/",
                    distant_src=True)


# ------------------------------------------------------------------------------
# Entrées locales
# ------------------------------------------------------------------------------
def lire_manifeste(chemin):
    if not Path(chemin).expanduser().is_file():
        sys.exit(f"Manifeste introuvable : {chemin}\n"
                 "Créez ce fichier CSV (modèle : entretiens_exemple.csv) avec les colonnes :\n"
                 "  entretien,video_sujet,video_psy,fix_sujet,fix_psy")
    base = Path(chemin).expanduser().resolve().parent
    with open(chemin, encoding="utf-8-sig", newline="") as f:
        entete = f.readline()
        f.seek(0)
        delim = ";" if entete.count(";") > entete.count(",") else ","
        lignes = list(csv.DictReader(f, delimiter=delim))
    liste = []
    for l in lignes:
        l = {(k or "").strip(): (v or "").strip() for k, v in l.items()}
        if not any(l.values()):
            continue
        manque = [c for c in ["entretien"] + CLES if not l.get(c)]
        if manque:
            sys.exit(f"Manifeste : valeurs manquantes {manque}")
        e = {"entretien": l["entretien"]}
        for k in CLES:
            p = Path(l[k]).expanduser()
            e[k] = str((p if p.is_absolute() else base / p).resolve())
        liste.append(e)
    return liste


def entrees_depuis_args(a):
    vals = {"video_sujet": a.video_sujet, "video_psy": a.video_psy,
            "fix_sujet": a.fix_sujet, "fix_psy": a.fix_psy}
    if not a.entretien or not all(vals.values()):
        sys.exit("Indiquez --manifeste, ou bien --entretien --video-sujet --video-psy "
                 "--fix-sujet --fix-psy")
    e = {"entretien": a.entretien}
    e.update({k: str(Path(v).expanduser().resolve()) for k, v in vals.items()})
    return [e]


def verifier_locaux(liste):
    absents = [(e["entretien"], k, e[k]) for e in liste for k in CLES if not Path(e[k]).exists()]
    if absents:
        sys.exit("Fichiers introuvables :\n" + "\n".join(f"  {n} / {k} : {p}" for n, k, p in absents))


def depots_effectifs(refs):
    depots = [list(d) for d in DEPOTS]
    for r in refs:
        nom, _, ref = r.partition("=")
        for d in depots:
            if d[0] == nom:
                d[2] = ref
                break
        else:
            sys.exit(f"--ref : dépôt inconnu '{nom}' (connus : {[d[0] for d in depots]})")
    for nom, url, _ in depots:
        if not re.fullmatch(r"[\w.\-]+", nom):
            sys.exit(f"Nom de dépôt invalide : {nom}")
    return depots


def dev(chemin, home):
    """~ -> dossier personnel distant."""
    return home + chemin[1:] if chemin.startswith("~") else chemin


# ------------------------------------------------------------------------------
# Opérations distantes
# ------------------------------------------------------------------------------
SCRIPT_DERNIER_LOT = (
    'lot=$(ls -1d "{lots}"/*/ 2>/dev/null | sort | tail -n 1 || true); lot=${{lot%/}}\n'
    '[ -n "$lot" ] || {{ echo "Aucun lot de pipelines : lancez d\'abord la commande lancer." >&2; exit 1; }}\n')


def commande_orchestrateur(sous_commande, lot_expr, options=""):
    return (f'python3 {lot_expr}/{DEPOT_ORCHESTRATION}/{FICHIER_ORCHESTRATEUR} {sous_commande} '
            f'--config {lot_expr}/{DEPOT_ORCHESTRATION}/{FICHIER_CONFIG} {options}\n')


def script_clonage(lot, depots):
    l = ['export GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=accept-new"',
         "export GIT_TERMINAL_PROMPT=0", f"mkdir -p {q(lot)}"]
    for nom, url, ref in depots:
        dest = q(f"{lot}/{nom}")
        if re.fullmatch(r"[0-9a-fA-F]{7,40}", ref):
            l.append(f"git clone --quiet {q(url)} {dest} && git -C {dest} checkout --quiet {q(ref)}")
        else:
            l.append(f"git clone --quiet --depth 1 --branch {q(ref)} {q(url)} {dest}")
        l.append(f'echo "  {nom} ({ref}) : $(git -C {dest} rev-parse --short HEAD)"')
    return "\n".join(l) + "\n"


def statut_json(hote, home):
    lots = dev(DOSSIER_LOTS_DISTANT, home)
    script = (SCRIPT_DERNIER_LOT.format(lots=lots)
              + commande_orchestrateur("statut", '"$lot"', "--json"))
    sortie = hote.executer(script, capture=True).stdout.strip().splitlines()
    for ligne in reversed(sortie):
        if ligne.startswith("["):
            return json.loads(ligne)
    return []


def fin_du_journal(hote, e, lignes=40, unite=None):
    """Affiche la fin du journal (le plus récent, ou celui de l'unité indiquée)."""
    logs = q(e["racine"]) + "/logs"
    if unite:
        choix = f'f={logs}/{q(unite)}.log\n'
    else:
        choix = f'f=$(ls -t {logs}/*.log 2>/dev/null | head -n 1 || true)\n'
    script = choix + ('if [ -n "$f" ] && [ -f "$f" ]; then echo "--- $f"; tail -n %d "$f"; '
                      'else echo "(aucun journal trouvé)"; fi\n' % lignes)
    hote.executer(script, check=False)


def recuperer(hote, statuts, noms, dest_local, avec_logs=False):
    faits = []
    for e in statuts:
        if noms and e["entretien"] not in noms:
            continue
        cible = Path(dest_local) / e["entretien"]
        if e["statut"] == "termine":
            hote.telecharger(e["resultats"], cible)
            print(f"  {e['entretien']} : résultats récupérés dans {cible}")
            faits.append(e["entretien"])
        else:
            print(f"  {e['entretien']} : {e['statut']} (résultats pas disponibles) {e['erreur']}".rstrip())
        if avec_logs:
            hote.telecharger(e["racine"] + "/logs", cible / "_suivi" / "logs")
            print(f"  {e['entretien']} : journaux récupérés dans {cible / '_suivi' / 'logs'}")
    return faits


# ------------------------------------------------------------------------------
# Commandes
# ------------------------------------------------------------------------------
def cmd_verifier(hote, a):
    home = hote.home()
    print(f"Connexion OK (dossier personnel distant : {home})")
    depots = depots_effectifs(a.ref)
    script = ('export GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=accept-new" GIT_TERMINAL_PROMPT=0\n'
              'echo "git      : $(git --version 2>&1 | head -n 1)"\n'
              'echo "python3  : $(python3 --version 2>&1)"\n'
              'if command -v oarsub >/dev/null; then echo "oarsub   : présent"; '
              'else echo "oarsub   : ABSENT (normal en mode essai local)"; fi\n')
    script += ('c=$(command -v conda || true)\n'
               'for x in "$HOME/miniforge3/bin/conda" "$HOME/miniconda3/bin/conda" "$HOME/anaconda3/bin/conda"; '
               'do if [ -z "$c" ] && [ -x "$x" ]; then c=$x; fi; done\n'
               'if [ -n "$c" ]; then echo "conda    : $c"; '
               'else echo "conda    : ABSENT (nécessaire pour l\'environnement de segmentation)"; fi\n'
               'if python3 -c "import ensurepip" 2>/dev/null; then echo "venv     : possible"; '
               'else echo "venv     : IMPOSSIBLE (paquet python3-venv manquant)"; fi\n')
    for nom, url, ref in depots:
        script += (f'if git ls-remote --exit-code {q(url)} >/dev/null 2>&1; then echo "dépôt {nom} : accessible"; '
                   f'else echo "dépôt {nom} : INACCESSIBLE ({url})"; fi\n')
    hote.executer(script)


def cmd_lancer(hote, a):
    liste = lire_manifeste(a.manifeste) if a.manifeste else entrees_depuis_args(a)
    verifier_locaux(liste)
    depots = depots_effectifs(a.ref)
    if any("VOTRE_COMPTE" in d[1] for d in depots) and not hote.local:
        sys.exit("Renseignez votre login et les URL de vos dépôts GitHub dans le fichier lanceur.ini "
                 "(à placer à côté de lancer_entretiens.py ; modèle : lanceur_exemple.ini).")

    home = hote.home()
    lot_id = time.strftime("%Y%m%d-%H%M%S") + "-" + secrets.token_hex(2)   # unique même à la seconde près
    lot = f"{dev(DOSSIER_LOTS_DISTANT, home)}/{lot_id}"
    print(f"== 1/4  Récupération des pipelines depuis GitHub (lot {lot_id})")
    hote.executer(script_clonage(lot, depots))

    print("   Contrôle de la configuration")
    r = hote.executer(commande_orchestrateur("controler", q(lot)), capture=True, check=False)
    sys.stdout.write(r.stdout or "")
    sys.stderr.write(r.stderr or "")
    if r.returncode != 0:
        if "invalid choice" in (r.stderr or ""):
            sys.exit("\nL'orchestrateur cloné depuis GitHub est trop ancien (il ne connaît pas la commande "
                     "'controler').\nPoussez la dernière version de orchestrateur.py et config.ini dans le dépôt "
                     f"'{DEPOT_ORCHESTRATION}', puis relancez.")
        sys.exit("\nArrêt avant l'envoi des vidéos : corrigez lanceur.ini ([depots]) ou config.ini, puis relancez.\n"
                 f"Pour voir ce qui a été cloné : python3 {Path(__file__).name} arbre")
    print("== 2/4  Envoi des entrées")
    racine_entrees = dev(DOSSIER_ENTREES_DISTANT, home)
    sortie_csv = io.StringIO()
    w = csv.writer(sortie_csv, lineterminator="\n")
    w.writerow(["entretien"] + CLES)
    for e in liste:
        ligne = [e["entretien"]]
        for k in CLES:
            regardeur = k.split("_", 1)[1]
            dest = f"{racine_entrees}/{nettoyer(e['entretien'])}/{regardeur}/{nettoyer(os.path.basename(e[k]))}"
            print(f"  {e['entretien']} / {k}")
            hote.envoyer(e[k], dest)
            ligne.append(dest)
        w.writerow(ligne)
    manifeste_distant = f"{lot}/entretiens.csv"
    hote.executer(f"cat > {q(manifeste_distant)} <<'FIN_MANIFESTE'\n{sortie_csv.getvalue()}FIN_MANIFESTE\n")

    print("== 3/4  Soumission des entretiens")
    options = f"--manifeste {q(manifeste_distant)}" + (" --simuler" if a.simuler else "") \
        + (" --forcer" if a.forcer else "")
    hote.executer(commande_orchestrateur("soumettre", q(lot), options))
    if a.simuler:
        return

    noms = [e["entretien"] for e in liste]
    dest_local = Path(os.path.expanduser(a.sortie or DOSSIER_RESULTATS))
    if not a.attendre:
        print(f"\nSoumis. Suivi : python3 {Path(__file__).name} statut\n"
              f"Résultats : python3 {Path(__file__).name} recuperer --sortie {dest_local}")
        return

    print(f"== 4/4  Attente des résultats (vérification toutes les {INTERVALLE_ATTENTE_MIN} min ; "
          f"Ctrl+C pour arrêter, les jobs continuent sur Grid'5000)")
    attendre(hote, home, noms, dest_local)


def attendre(hote, home, noms, dest_local):
    termines, abandonnes, relances = set(), set(), {n: 0 for n in noms}
    while len(termines) + len(abandonnes) < len(noms):
        try:
            statuts = {e["entretien"]: e for e in statut_json(hote, home)}
            for n in noms:
                if n in termines or n in abandonnes or n not in statuts:
                    continue
                e = statuts[n]
                if e["statut"] == "termine":
                    recuperer(hote, [e], [n], dest_local)
                    termines.add(n)
                elif e["statut"] == "interrompu" and relances[n] < RELANCES_AUTO:
                    relances[n] += 1
                    print(f"  {n} : interrompu (temps écoulé) -> relance automatique {relances[n]}/{RELANCES_AUTO}")
                    lots = dev(DOSSIER_LOTS_DISTANT, home)
                    hote.executer(SCRIPT_DERNIER_LOT.format(lots=lots)
                                  + commande_orchestrateur("relancer", '"$lot"', q(n)))
                elif e["statut"] in ("echec", "interrompu"):
                    print(f"  {n} : {e['statut']} - {e['erreur']}")
                    fin_du_journal(hote, e)
                    print(f"  (journal complet : python3 {Path(__file__).name} recuperer {n} --avec-logs)")
                    abandonnes.add(n)
            restants = [n for n in noms if n not in termines and n not in abandonnes]
            if restants:
                print(f"  [{time.strftime('%H:%M')}] en cours : {', '.join(restants)}")
        except (SystemExit, subprocess.CalledProcessError, OSError) as exc:
            print(f"  (connexion perdue, nouvel essai plus tard : {exc})")
        if len(termines) + len(abandonnes) < len(noms):
            time.sleep(INTERVALLE_ATTENTE_MIN * 60)
    print(f"\nTerminé : {len(termines)} entretien(s) récupéré(s), {len(abandonnes)} en échec.")


def cmd_statut(hote, a):
    home = hote.home()
    statuts = statut_json(hote, home)
    if not statuts:
        print("Aucun entretien.")
    for e in statuts:
        print(f"{e['entretien']:<20} {e['statut']:<11} étapes {e['unites_terminees']}/{e['unites_total']}"
              f"  essais={e['tentatives']}  jobs={','.join(map(str, e['job_ids'])) or '-'}"
              f"  {e['erreur']}".rstrip())


def cmd_recuperer(hote, a):
    home = hote.home()
    dest = Path(os.path.expanduser(a.sortie or DOSSIER_RESULTATS))
    faits = recuperer(hote, statut_json(hote, home), a.entretiens, dest, a.avec_logs)
    if not faits:
        print("Rien à récupérer pour l'instant.")


def cmd_journal(hote, a):
    home = hote.home()
    statuts = {e["entretien"]: e for e in statut_json(hote, home)}
    if a.entretien not in statuts:
        sys.exit(f"Entretien inconnu : {a.entretien} (connus : {sorted(statuts)})")
    fin_du_journal(hote, statuts[a.entretien], a.lignes, a.unite)


def cmd_arbre(hote, a):
    """Arborescence du dernier lot de pipelines clonés (pour vérifier les chemins de config.ini)."""
    home = hote.home()
    lots = dev(DOSSIER_LOTS_DISTANT, home)
    hote.executer(SCRIPT_DERNIER_LOT.format(lots=lots)
                  + 'echo "Dernier lot : $lot"\n'
                  + f'find "$lot" -maxdepth {a.profondeur} -not -path "*/.git" -not -path "*/.git/*" '
                    '-not -path "*/.envs" -not -path "*/.envs/*" | sed "s#^$lot/##" | sort\n')


def cmd_relancer(hote, a):
    home = hote.home()
    lots = dev(DOSSIER_LOTS_DISTANT, home)
    hote.executer(SCRIPT_DERNIER_LOT.format(lots=lots)
                  + commande_orchestrateur("relancer", '"$lot"', " ".join(q(n) for n in a.entretiens)))


def main():
    p = argparse.ArgumentParser(description="Lanceur local des pipelines d'entretiens (Grid'5000).")
    sp = p.add_subparsers(dest="cmd", required=True)

    def commun(s):
        s.add_argument("--hote", help="Alias SSH ou 'local' (essai) ; remplace SSH_HOTE")
        s.add_argument("--ref", action="append", default=[], metavar="DEPOT=REF",
                       help="Branche/tag/commit d'un dépôt, ex. --ref analyse=ma-branche")

    s = sp.add_parser("verifier", help="Tester la connexion, git, python et l'accès aux dépôts")
    commun(s)
    s = sp.add_parser("lancer", help="Récupérer les pipelines, envoyer les entrées, soumettre")
    commun(s)
    s.add_argument("--manifeste")
    s.add_argument("--entretien")
    s.add_argument("--video-sujet")
    s.add_argument("--video-psy")
    s.add_argument("--fix-sujet")
    s.add_argument("--fix-psy")
    s.add_argument("--sortie", help="Dossier local des résultats (défaut : DOSSIER_RESULTATS)")
    s.add_argument("--attendre", action="store_true", help="Attendre et récupérer les résultats au fil de l'eau")
    s.add_argument("--simuler", action="store_true", help="Afficher les oarsub sans soumettre")
    s.add_argument("--forcer", action="store_true", help="Refaire même si déjà terminé")
    s = sp.add_parser("statut", help="Avancement des entretiens")
    commun(s)
    s = sp.add_parser("recuperer", help="Télécharger les résultats des entretiens terminés")
    commun(s)
    s.add_argument("entretiens", nargs="*")
    s.add_argument("--sortie")
    s.add_argument("--avec-logs", action="store_true", help="Télécharger aussi les journaux")
    s = sp.add_parser("journal", help="Afficher la fin du journal d'un entretien (diagnostic d'échec)")
    commun(s)
    s.add_argument("entretien")
    s.add_argument("--unite", help="Journal d'une étape précise, ex. segmentation_sujet")
    s.add_argument("--lignes", type=int, default=40)
    s = sp.add_parser("arbre", help="Afficher l'arborescence des pipelines clonés (dernier lot)")
    commun(s)
    s.add_argument("--profondeur", type=int, default=3)
    s = sp.add_parser("relancer", help="Resoumettre des entretiens en échec ou interrompus")
    commun(s)
    s.add_argument("entretiens", nargs="*")

    a = p.parse_args()
    charger_reglages()
    hote = Hote(a.hote or SSH_HOTE)
    {"verifier": cmd_verifier, "lancer": cmd_lancer, "statut": cmd_statut,
     "recuperer": cmd_recuperer, "relancer": cmd_relancer,
     "journal": cmd_journal, "arbre": cmd_arbre}[a.cmd](hote, a)


if __name__ == "__main__":
    main()
