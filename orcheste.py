#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Orchestrateur des 5 pipelines (Grid'5000 / OAR).

Ordre d'exécution, pour chaque entretien :
    1. segmentation          (vidéo sujet, vidéo psy)
    2. synchro_video_fichier (vidéo segmentée + fichier de fixations eye-tracker)
    3. mesures_manquantes    (résultat de la synchro + vidéo d'origine)
    4. transcription         (une des vidéos d'origine)
    5. analyse               (résultats de mesures_manquantes + transcription)
                             -> répartition des saccades, répartition des fixations,
                                synchronisation fixations / transcription

Chaque entretien = UN job OAR de 12 h. Les jobs sont enchaînés avec la dépendance
OAR "--anterior" : le job suivant démarre dès que le précédent est terminé (sans
attendre la fin des 12 h). Un état est enregistré après chaque étape : un entretien
interrompu (walltime atteint, erreur) est repris là où il s'était arrêté.

Commandes (à lancer depuis le frontend Grid'5000) :
    python3 orchestrateur.py soumettre --config config.ini --manifeste entretiens.csv
    python3 orchestrateur.py soumettre --config config.ini --entretien BOC.066 \\
        --video-sujet V_S.mp4 --video-psy V_P.mp4 --fix-sujet F_S.csv --fix-psy F_P.csv
    python3 orchestrateur.py statut    --config config.ini
    python3 orchestrateur.py relancer  --config config.ini [ENTRETIEN ...]
    python3 orchestrateur.py executer  --config config.ini --entretien BOC.066
        (appelée par OAR dans le job ; utile aussi pour un essai direct)

Options de soumettre : --simuler (affiche les commandes oarsub sans les lancer),
--local (exécute tout de suite, sans OAR), --apres JOB_ID, --forcer.

Dépendances : bibliothèque standard uniquement.
"""

import argparse
import configparser
import csv
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ORDRE = ["segmentation", "synchro_video_fichier", "mesures_manquantes",
         "transcription", "analyse"]
REGARDEURS = ["sujet", "psy"]
CLES_ENTREES = [f"{t}_{r}" for t in ("video", "fix") for r in REGARDEURS]

CODE_OK, CODE_ECHEC, CODE_INTERROMPU = 0, 1, 3


def log(msg):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def nettoyer_nom(nom):
    return re.sub(r"[^\w.\-]+", "_", nom).strip("_") or "entretien"


def secondes(hms):
    """'12:00:00' -> 43200 (accepte aussi 'h:mm' ou 'h')."""
    p = [int(x) for x in hms.split(":")]
    p += [0] * (3 - len(p))
    return p[0] * 3600 + p[1] * 60 + p[2]


# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
class Config:
    def __init__(self, chemin):
        self.chemin = Path(chemin).expanduser().resolve()
        cp = configparser.ConfigParser(interpolation=None)
        cp.optionxform = str
        with open(self.chemin, encoding="utf-8") as f:
            cp.read_file(f)

        g = cp["general"]
        self.racine_travail = Path(os.path.expanduser(g["racine_travail"])).resolve()
        self.preparation = g.get("preparation", "").strip()
        self.python = g.get("python", "python3").strip()
        self.scripts = Path(os.path.expanduser(g.get("scripts", "").strip()
                                               or Path(__file__).parent)).resolve()
        self.video_transcription = g.get("video_transcription", "sujet").strip()
        if self.video_transcription not in REGARDEURS:
            sys.exit(f"video_transcription doit valoir l'une de {REGARDEURS}")

        o = cp["grid5000"] if cp.has_section("grid5000") else {}
        self.ressources = o.get("ressources", "host=1").strip()
        self.walltime = o.get("walltime", "12:00:00").strip()
        self.options_oar = o.get("options_oar", "").strip()
        self.marge_min = int(o.get("marge_securite_min", "10"))

        self.etapes = {}
        for e in ORDRE:
            if not cp.has_section(e):
                sys.exit(f"Section [{e}] manquante dans {self.chemin}")
            s = cp[e]
            cmds = [l.strip() for l in s.get("commandes", "").splitlines() if l.strip()]
            if not cmds:
                sys.exit(f"[{e}] : 'commandes' est vide")
            portee = s.get("portee", "entretien").strip()
            if portee not in ("regardeur", "entretien"):
                sys.exit(f"[{e}] : portee doit valoir 'regardeur' ou 'entretien'")
            self.etapes[e] = {"commandes": cmds, "portee": portee,
                              "sortie": s.get("sortie", "").strip()}

    def dossier(self, entretien):
        return self.racine_travail / nettoyer_nom(entretien)


def dossier_unite(racine, etape, reg=None):
    d = racine / f"{ORDRE.index(etape) + 1:02d}_{etape}"
    return d / reg if reg else d


def trouver_sortie(cfg, racine, etape, reg=None):
    """Fichier produit par une étape (le plus récent correspondant au motif 'sortie')."""
    motif = cfg.etapes[etape]["sortie"]
    if not motif:
        return None
    d = dossier_unite(racine, etape, reg)
    trouves = sorted(d.glob(motif), key=lambda p: p.stat().st_mtime)
    return trouves[-1] if trouves else None


# ------------------------------------------------------------------------------
# Variables utilisables dans les commandes : {video}, {fix}, {sortie}, ...
# ------------------------------------------------------------------------------
def decouper(nom):
    """'segmentation' -> (segmentation, None) ; 'segmentation_psy' -> (segmentation, 'psy')."""
    if nom in ORDRE:
        return nom, None
    for r in REGARDEURS:
        if nom.endswith("_" + r) and nom[: -len(r) - 1] in ORDRE:
            return nom[: -len(r) - 1], r
    return None, None


class Contexte:
    """Valeurs insérées dans les commandes (automatiquement protégées pour le shell)."""

    def __init__(self, cfg, racine, entrees, etape, reg):
        self.cfg, self.racine, self.etape, self.reg = cfg, racine, etape, reg
        self.base = {
            "entretien": entrees["entretien"], "racine": racine,
            "resultats": racine / "resultats", "scripts": cfg.scripts,
            "python": cfg.python,
            "video_transcription": entrees[f"video_{cfg.video_transcription}"],
        }
        self.base.update({k: entrees[k] for k in CLES_ENTREES})
        portee = cfg.etapes[etape]["portee"]
        if portee == "regardeur":
            self.base.update({"regardeur": reg, "video": entrees[f"video_{reg}"],
                              "fix": entrees[f"fix_{reg}"],
                              "sortie": dossier_unite(racine, etape, reg)})
        else:
            self.base["sortie"] = dossier_unite(racine, etape)

    def valeur(self, cle):
        if cle in self.base:
            return self.base[cle]
        if cle.startswith("dir_"):
            e, r = decouper(cle[4:])
            if e:
                return dossier_unite(self.racine, e, r)
        e, r = decouper(cle)
        if e:
            if r is None and self.cfg.etapes[e]["portee"] == "regardeur":
                if not self.reg:
                    raise KeyError(f"{{{cle}}} : précisez _sujet ou _psy")
                r = self.reg
            f = trouver_sortie(self.cfg, self.racine, e, r)
            if f is None:
                raise KeyError(f"{{{cle}}} : aucun fichier de sortie trouvé pour l'étape "
                               f"'{e}' (motif 'sortie' = '{self.cfg.etapes[e]['sortie']}')")
            return f
        raise KeyError(f"variable inconnue : {{{cle}}}")

    def __getitem__(self, cle):
        return shlex.quote(str(self.valeur(cle)))


# ------------------------------------------------------------------------------
# État d'un entretien (reprise)
# ------------------------------------------------------------------------------
def chemin_etat(racine):
    return racine / "etat.json"


def charger_etat(racine):
    p = chemin_etat(racine)
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def sauver_etat(racine, etat):
    etat["maj"] = datetime.now().isoformat(timespec="seconds")
    tmp = chemin_etat(racine).with_suffix(".tmp")
    tmp.write_text(json.dumps(etat, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(chemin_etat(racine))


def initialiser_etat(cfg, entrees, forcer=False):
    racine = cfg.dossier(entrees["entretien"])
    racine.mkdir(parents=True, exist_ok=True)
    (racine / "logs").mkdir(exist_ok=True)
    etat = charger_etat(racine)
    if etat and etat.get("statut") == "termine" and not forcer:
        return None
    if etat is None or forcer:
        etat = {"entretien": entrees["entretien"], "tentatives": 0, "job_ids": [],
                "unites": {}}
    etat["entrees"] = entrees
    etat["statut"] = "soumis"
    sauver_etat(racine, etat)
    return etat


def liste_unites(cfg):
    unites = []
    for e in ORDRE:
        if cfg.etapes[e]["portee"] == "regardeur":
            unites += [(e, r) for r in REGARDEURS]
        else:
            unites.append((e, None))
    return unites


def cle_unite(etape, reg):
    return f"{etape}:{reg}" if reg else etape


# ------------------------------------------------------------------------------
# Exécution d'un entretien (dans le job OAR)
# ------------------------------------------------------------------------------
_ARRET = {"demande": False, "proc": None}


def _gestionnaire(signum, frame):
    _ARRET["demande"] = True
    p = _ARRET["proc"]
    if p is not None and p.poll() is None:
        try:
            os.killpg(p.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass


def lancer_unite(cfg, racine, entrees, etape, reg):
    """Lance les commandes d'une unité ; retourne le code de retour."""
    ctx = Contexte(cfg, racine, entrees, etape, reg)
    commandes = [c.format_map(ctx) for c in cfg.etapes[etape]["commandes"]]
    script = "set -euo pipefail\n"
    if cfg.preparation:
        script += cfg.preparation + "\n"
    script += "\n".join(commandes) + "\n"

    ctx.base["sortie"].mkdir(parents=True, exist_ok=True)
    journal = racine / "logs" / f"{cle_unite(etape, reg).replace(':', '_')}.log"
    with open(journal, "ab") as f:
        f.write(f"\n===== {datetime.now():%Y-%m-%d %H:%M:%S} : {cle_unite(etape, reg)} =====\n"
                .encode())
        f.write(("\n".join(commandes) + "\n-----\n").encode())
        f.flush()
        proc = subprocess.Popen(["bash", "-c", script], cwd=racine, stdout=f,
                                stderr=subprocess.STDOUT, start_new_session=True)
        _ARRET["proc"] = proc
        code = proc.wait()
        _ARRET["proc"] = None
    return code, journal


def executer(cfg, entretien):
    racine = cfg.dossier(entretien)
    etat = charger_etat(racine)
    if etat is None:
        sys.exit(f"Entretien '{entretien}' inconnu : lancez d'abord 'soumettre'.")
    entrees = etat["entrees"]

    for sig in (signal.SIGTERM, signal.SIGINT, getattr(signal, "SIGUSR2", signal.SIGTERM)):
        signal.signal(sig, _gestionnaire)

    debut = time.time()
    walltime = int(os.environ.get("OAR_JOB_WALLTIME_SECONDS", secondes(cfg.walltime)))
    limite = debut + walltime - cfg.marge_min * 60

    etat["statut"] = "en_cours"
    etat["tentatives"] += 1
    if os.environ.get("OAR_JOB_ID"):
        etat["job_en_cours"] = os.environ["OAR_JOB_ID"]
    sauver_etat(racine, etat)
    (racine / "resultats").mkdir(exist_ok=True)
    log(f"Entretien {entretien} : démarrage (tentative {etat['tentatives']})")

    manquants = [k for k in CLES_ENTREES if not Path(entrees[k]).exists()]
    if manquants:
        log(f"ERREUR : fichiers d'entrée introuvables : "
            f"{[entrees[k] for k in manquants]}")
        etat["statut"] = "echec"
        etat["erreur"] = f"entrées introuvables : {manquants}"
        sauver_etat(racine, etat)
        return CODE_ECHEC

    for etape, reg in liste_unites(cfg):
        cle = cle_unite(etape, reg)
        u = etat["unites"].get(cle, {})
        if u.get("statut") == "termine" and (
                not cfg.etapes[etape]["sortie"] or trouver_sortie(cfg, racine, etape, reg)):
            log(f"  {cle} : déjà terminé, ignoré")
            continue

        if _ARRET["demande"] or time.time() > limite:
            log(f"  Temps presque écoulé avant '{cle}' : arrêt propre "
                f"(reprise avec 'relancer').")
            etat["statut"] = "interrompu"
            etat["erreur"] = f"temps écoulé avant {cle}"
            sauver_etat(racine, etat)
            return CODE_INTERROMPU

        log(f"  {cle} : début")
        t0 = time.time()
        etat["unites"][cle] = {"statut": "en_cours",
                               "debut": datetime.now().isoformat(timespec="seconds")}
        sauver_etat(racine, etat)
        try:
            code, journal = lancer_unite(cfg, racine, entrees, etape, reg)
        except KeyError as exc:      # variable {..} introuvable dans une commande
            code, journal = 2, racine / "logs"
            etat["erreur"] = str(exc.args[0])
            log(f"  {cle} : ERREUR de configuration : {exc.args[0]}")

        duree = round(time.time() - t0)
        if code == 0 and cfg.etapes[etape]["sortie"] and not trouver_sortie(cfg, racine, etape, reg):
            code = 4
            etat["erreur"] = f"{cle} : aucun fichier de sortie ('{cfg.etapes[etape]['sortie']}')"

        if code == 0:
            etat["unites"][cle].update(statut="termine", duree_s=duree, journal=str(journal))
            etat.pop("erreur", None)
            sauver_etat(racine, etat)
            log(f"  {cle} : terminé en {duree // 60} min")
            continue

        if _ARRET["demande"]:
            etat["unites"][cle].update(statut="interrompu", duree_s=duree)
            etat["statut"] = "interrompu"
            etat["erreur"] = f"{cle} interrompu par le signal de fin de walltime"
            sauver_etat(racine, etat)
            log(f"  {cle} : interrompu (fin de walltime)")
            return CODE_INTERROMPU

        etat["unites"][cle].update(statut="echec", duree_s=duree, journal=str(journal))
        etat["statut"] = "echec"
        etat.setdefault("erreur", f"{cle} : code de retour {code}")
        sauver_etat(racine, etat)
        log(f"  {cle} : ÉCHEC (code {code}) - voir {journal}")
        return CODE_ECHEC

    etat["statut"] = "termine"
    etat.pop("erreur", None)
    sauver_etat(racine, etat)
    log(f"Entretien {entretien} : TERMINÉ en {round((time.time() - debut) / 60)} min. "
        f"Résultats : {racine / 'resultats'}")
    return CODE_OK


# ------------------------------------------------------------------------------
# Soumission à OAR
# ------------------------------------------------------------------------------
def commande_oarsub(cfg, entretien, anterieur):
    logs = cfg.dossier(entretien) / "logs"
    cmd = ["oarsub", "-l", f"{cfg.ressources},walltime={cfg.walltime}",
           "-n", f"entretien_{nettoyer_nom(entretien)}",
           "-O", str(logs / "oar_%jobid%.out"), "-E", str(logs / "oar_%jobid%.err"),
           "--checkpoint", str(cfg.marge_min * 60)]
    cmd += shlex.split(cfg.options_oar)
    if anterieur:
        cmd += ["-a", str(anterieur)]
    cmd.append(" ".join(shlex.quote(x) for x in [
        cfg.python, str(Path(__file__).resolve()), "executer",
        "--config", str(cfg.chemin), "--entretien", entretien]))
    return cmd


def soumettre_liste(cfg, liste_entrees, apres=None, simuler=False, local=False, forcer=False):
    precedent = apres
    n_sim = 0
    for entrees in liste_entrees:
        nom = entrees["entretien"]
        etat = initialiser_etat(cfg, entrees, forcer=forcer)
        if etat is None:
            log(f"{nom} : déjà terminé (utilisez --forcer pour refaire)")
            continue
        if local:
            executer(cfg, nom)
            continue

        cmd = commande_oarsub(cfg, nom, precedent)
        if simuler:
            n_sim += 1
            job_id = f"SIMULE{n_sim}"
            print("  " + " ".join(shlex.quote(c) for c in cmd))
        else:
            if shutil.which("oarsub") is None:
                sys.exit("oarsub introuvable : lancez 'soumettre' depuis le frontend "
                         "Grid'5000 (ou utilisez --local / --simuler).")
            r = subprocess.run(cmd, capture_output=True, text=True)
            m = re.search(r"OAR_JOB_ID=(\d+)", r.stdout)
            if r.returncode != 0 or not m:
                sys.exit(f"Échec de oarsub pour {nom} :\n{r.stdout}\n{r.stderr}")
            job_id = m.group(1)
        etat = charger_etat(cfg.dossier(nom))
        etat["job_ids"].append(job_id)
        sauver_etat(cfg.dossier(nom), etat)
        log(f"{nom} : job {job_id} soumis" + (f" (après le job {precedent})" if precedent else ""))
        precedent = job_id


# ------------------------------------------------------------------------------
# Entrées : manifeste CSV ou options en ligne de commande
# ------------------------------------------------------------------------------
def chemin_abs(valeur, base):
    p = Path(valeur).expanduser()
    return str((p if p.is_absolute() else base / p).resolve())


def lire_manifeste(chemin):
    base = Path(chemin).resolve().parent
    with open(chemin, encoding="utf-8-sig", newline="") as f:
        entete = f.readline()
        f.seek(0)
        delim = ";" if entete.count(";") > entete.count(",") else ","
        lignes = list(csv.DictReader(f, delimiter=delim))
    colonnes = ["entretien"] + CLES_ENTREES
    resultat = []
    for l in lignes:
        l = {(k or "").strip(): (v or "").strip() for k, v in l.items()}
        if not any(l.values()):
            continue
        manque = [c for c in colonnes if not l.get(c)]
        if manque:
            sys.exit(f"Manifeste : colonnes/valeurs manquantes {manque} "
                     f"(attendu : {colonnes})")
        e = {"entretien": l["entretien"]}
        e.update({k: chemin_abs(l[k], base) for k in CLES_ENTREES})
        resultat.append(e)
    return resultat


def entrees_depuis_args(a):
    vals = {"video_sujet": a.video_sujet, "video_psy": a.video_psy,
            "fix_sujet": a.fix_sujet, "fix_psy": a.fix_psy}
    if not a.entretien or not all(vals.values()):
        sys.exit("Indiquez --manifeste, ou bien --entretien --video-sujet --video-psy "
                 "--fix-sujet --fix-psy")
    e = {"entretien": a.entretien}
    e.update({k: chemin_abs(v, Path.cwd()) for k, v in vals.items()})
    return [e]


def verifier_entrees(liste):
    absents = [(e["entretien"], k, e[k]) for e in liste for k in CLES_ENTREES
               if not Path(e[k]).exists()]
    if absents:
        sys.exit("Fichiers introuvables :\n" + "\n".join(f"  {n} / {k} : {p}"
                                                        for n, k, p in absents))


# ------------------------------------------------------------------------------
# Statut
# ------------------------------------------------------------------------------
def afficher_statut(cfg):
    total = len(liste_unites(cfg))
    lignes = []
    for p in sorted(cfg.racine_travail.glob("*/etat.json")):
        e = json.loads(p.read_text(encoding="utf-8"))
        faites = sum(1 for u in e["unites"].values() if u.get("statut") == "termine")
        lignes.append((e["entretien"], e["statut"], f"{faites}/{total}", e["tentatives"],
                       ",".join(map(str, e["job_ids"])) or "-", e.get("maj", ""),
                       e.get("erreur", "")))
    if not lignes:
        print("Aucun entretien.")
        return
    ent = ("entretien", "statut", "étapes", "essais", "jobs OAR", "maj", "erreur")
    larg = [max(len(str(x[i])) for x in lignes + [ent]) for i in range(len(ent))]
    for l in [ent] + lignes:
        print("  ".join(str(v).ljust(larg[i]) for i, v in enumerate(l)).rstrip())


# ------------------------------------------------------------------------------
# Programme principal
# ------------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser(description="Orchestrateur des 5 pipelines (Grid'5000).")
    sp = p.add_subparsers(dest="cmd", required=True)

    def commun(s):
        s.add_argument("--config", required=True, help="Fichier de configuration (.ini)")

    s = sp.add_parser("soumettre", help="Soumettre un ou plusieurs entretiens")
    commun(s)
    s.add_argument("--manifeste", help="CSV : entretien, video_sujet, video_psy, fix_sujet, fix_psy")
    s.add_argument("--entretien")
    s.add_argument("--video-sujet")
    s.add_argument("--video-psy")
    s.add_argument("--fix-sujet")
    s.add_argument("--fix-psy")
    s.add_argument("--apres", help="Démarrer après ce job OAR")
    s.add_argument("--simuler", action="store_true", help="Afficher les oarsub sans les lancer")
    s.add_argument("--local", action="store_true", help="Exécuter sans OAR, un entretien après l'autre")
    s.add_argument("--forcer", action="store_true", help="Refaire même si déjà terminé")

    s = sp.add_parser("executer", help="Exécuter un entretien (utilisé par OAR)")
    commun(s)
    s.add_argument("--entretien", required=True)

    s = sp.add_parser("statut", help="Afficher l'avancement")
    commun(s)

    s = sp.add_parser("relancer", help="Resoumettre les entretiens incomplets")
    commun(s)
    s.add_argument("entretiens", nargs="*", help="Entretiens à relancer (défaut : tous les incomplets)")
    s.add_argument("--apres")
    s.add_argument("--simuler", action="store_true")
    s.add_argument("--local", action="store_true")

    a = p.parse_args()
    cfg = Config(a.config)

    if a.cmd == "executer":
        sys.exit(executer(cfg, a.entretien))
    if a.cmd == "statut":
        afficher_statut(cfg)
        return
    if a.cmd == "soumettre":
        liste = lire_manifeste(a.manifeste) if a.manifeste else entrees_depuis_args(a)
        verifier_entrees(liste)
        soumettre_liste(cfg, liste, apres=a.apres, simuler=a.simuler,
                        local=a.local, forcer=a.forcer)
        return
    if a.cmd == "relancer":
        liste = []
        for pe in sorted(cfg.racine_travail.glob("*/etat.json")):
            e = json.loads(pe.read_text(encoding="utf-8"))
            if a.entretiens and e["entretien"] not in a.entretiens:
                continue
            if e["statut"] in ("echec", "interrompu") or (a.entretiens and e["statut"] != "termine"):
                liste.append(e["entrees"])
        if not liste:
            print("Rien à relancer.")
            return
        soumettre_liste(cfg, liste, apres=a.apres, simuler=a.simuler, local=a.local)


if __name__ == "__main__":
    main()
