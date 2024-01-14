#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import sys
import time

from rewriterFromCSV import beautifyFlights, toJson

SUMMARY_MEAN_PATH = "../Data/rewrite_mean"
SUMMARY_ALPHA_PATH = "../Data/rewrite_alpha"
ASSOC_PATH = "../Data/assoc"


def doAssoc(v: list[str], r: dict[str, float], rv: dict[str, float]) -> dict[str, float]:
    start_time = time.time()
    result: dict[str, float] = dict()
    for term in r.keys():
        nb_summarized_terms = len(v)
        result[term] = assoc([term], r, rv, nb_summarized_terms)
    end_time = time.time()
    print(f"Finished assoc in {round(end_time - start_time, 2)} seconds")
    return result


def cover(vp: list[str], summary: dict[str, float], nb_terms_summarized: int = None):
    result = 0
    for term in vp:
        result += summary[term] if term in summary.keys() else 0
    return result / len(summary.keys()) if nb_terms_summarized is None else nb_terms_summarized


def dep(vp: list[str], r: dict[str, float], rv: dict[str, float], nb_terms_summarized):
    cover_vp_in_rv = cover(vp, rv, nb_terms_summarized)
    cover_vp_in_r = cover(vp, r)
    return cover_vp_in_rv / cover_vp_in_r if cover_vp_in_r != 0 else 0


def assoc(vp: list[str], r: dict[str, float], rv: dict[str, float], nb_terms_summarized):
    dep_vp = dep(vp, r, rv, nb_terms_summarized)
    return 0 if dep_vp <= 1 else 1 - (1 / dep_vp)


def extractTerms(rv: dict[str, float]):
    return [term for term, degree in rv.items() if degree >= 0.999]


def unBeautify(summary: dict[str, dict[str, float]]):
    return {f"{key}.{modality}": degree for key, val in summary.items() for modality, degree in val.items()}


def fromJson(path: str) -> dict[str, dict[str, float]]:
    try:
        with open(path, 'r') as rp:
            content = rp.read()
            return json.loads(content)
    except IOError:
        print("Error loading files")


if __name__ == "__main__":

    if (len(sys.argv) != 1 and len(sys.argv) != 3) or "--help" in sys.argv:
        print("Usage : python assoc.py [<path_to_R>] [<path_to_Rv>]")
        print("Defaults :")
        print("\t<path_to_R> = '..\\Data\\rewritten_mean.json'")
        print("\t<path_to_Rv> = '..\\Data\\rewritten_alpha.json'")
        exit(0)

    print("Doing assoc")

    rPath = sys.argv[1] if len(sys.argv) == 3 else f"{SUMMARY_MEAN_PATH}.json"
    rvPath = sys.argv[2] if len(sys.argv) == 3 else f"{SUMMARY_ALPHA_PATH}.json"
    print(f"Path to R : {rPath}")
    print(f"Path to Rv : {rvPath}")

    R = unBeautify(fromJson(rPath))
    Rv = unBeautify(fromJson(rvPath))
    terms = extractTerms(Rv)
    print(f"Terms : {terms}")

    assocs = doAssoc(terms, R, Rv)

    toJson(beautifyFlights(assocs), ASSOC_PATH)
