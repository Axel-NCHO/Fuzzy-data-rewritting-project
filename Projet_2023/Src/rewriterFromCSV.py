#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import time

from vocabulary import *
from flight import Flight
from flightsSummarizers import IFlightSummarizer, FlightSummarizerMean, FlightsSummarizerSatisfaction
import multiprocessing

SUMMARY_MEAN_PATH = "../Data/rewrite_mean"
SUMMARY_ALPHA_PATH = "../Data/rewrite_alpha"
ASSOC_PATH = "../Data/assoc"


class RewriterFromCSV(object):

    def __init__(self, voc: Vocabulary, df: str):
        """
        Translate a dataFile using a given vocabulary
        """
        self.vocabulary: Vocabulary = voc
        self.dataFile: str = df

    def rewrite(self, process_id: int, batch: list[str], nb_flights: int, out_partial_summaries: list,
                out_partial_nb_flights_satisfies_terms: list, terms_statisfaction: list[str] = None):
        summarize_with_satisfaction: bool = terms_statisfaction is not None
        summarizer: IFlightSummarizer = FlightsSummarizerSatisfaction(
            terms_statisfaction) if summarize_with_satisfaction \
            else FlightSummarizerMean(nb_flights)

        for line in batch:
            line = line.strip()
            if line != "" and line[0] != "#" and not line.startswith("Year"):
                f = Flight(line, self.vocabulary)
                summarizer.addFlight(f.rewrite())

        if summarize_with_satisfaction:
            summary, nb_flights_satisfies_terms = summarizer.getSummary()
            out_partial_summaries[process_id] = summary
            out_partial_nb_flights_satisfies_terms[process_id] = nb_flights_satisfies_terms

        else:
            out_partial_summaries[process_id] = summarizer.getSummary()
            out_partial_nb_flights_satisfies_terms[process_id] = None

    def readAndRewrite(self, alpha_satisfaction: float = None, terms_satisfaction: list[str] = None) -> dict[
        str, float]:
        """
        Tested machine config: \n
        - CPU Intel Core i7-11800H
        - RAM 16B \n
        Elapsed time until completion with batch size = 10_000 : \n
        - < 120 seconds (< 2 minutes) on full data set (7_009_728 flights)
        """
        try:
            with open(self.dataFile, 'r') as source:
                lines = source.readlines()
                nb_flights = len(lines) - 1  # Skipping the first line of the csv file
                print("Dividing data into batches for parallelization")
                batches = makeBatches(lines, 10_000)
                nb_batches = len(batches)
                print(f"Nb batches : {nb_batches}")

                jobs = []
                process_manager = multiprocessing.Manager()
                partial_summaries = process_manager.list(range(nb_batches))
                partial_nb_flights_satisfies_all_terms = process_manager.list(range(nb_batches))
                for batch, process_id in zip(batches, range(nb_flights)):
                    p = multiprocessing.Process(target=self.rewrite,
                                                args=[process_id, batch, nb_flights, partial_summaries,
                                                      partial_nb_flights_satisfies_all_terms, terms_satisfaction])
                    jobs.append(p)
                    p.start()

                for process in jobs:
                    process.join()

            summary = partial_summaries[0]
            if alpha_satisfaction is not None and terms_satisfaction is not None:
                # The FlightSummarizerSatisfaction object returns non-normalized values
                # So, while combining the partial summaries, we need to divide the degrees by the sum of the
                # partial numbers of flights that satisfy the conjunction of terms
                nb_flights_satisfies_all_terms = sum(partial_nb_flights_satisfies_all_terms)
                for i in range(1, len(partial_summaries)):
                    if i == 1:
                        summary = {key: (summary.get(key, 0) + partial_summaries[i].get(key,
                                                                                        0)) / nb_flights_satisfies_all_terms
                                   for key in set(summary) | set(partial_summaries[i])}
                    else:
                        summary = {key: summary.get(key, 0) + (
                                partial_summaries[i].get(key, 0) / nb_flights_satisfies_all_terms) for key in
                                   set(summary) | set(partial_summaries[i])}
                # Remove all terms having a degree lesser than alpha
                summary = {key: summary[key] for key in summary if summary[key] > alpha_satisfaction}

            else:
                # The values returned by the FlightSummarizerMean object are already normalized.
                # We just have to combine the partial summaries.
                for i in range(1, len(partial_summaries)):
                    summary = {key: summary.get(key, 0) + partial_summaries[i].get(key, 0) for key in
                               set(summary) | set(partial_summaries[i])}

            return summary

        except:
            raise Exception("Error while loading the dataFile %s" % self.dataFile)


def beautifyFlights(flights: dict[str, float]):
    result = dict()
    for key, val in flights.items():
        father, child = key.split(".")
        if father in result.keys():
            result[father][child] = val
        else:
            result[father] = {child: val}
    return result


def toJson(data, path: str):
    path = f"{path}.json" if not path.endswith(".json") else path
    with open(path, 'w') as file:
        json.dump(data, file)
        print(f"Successfully saved {path}")


def makeBatches(data: list[str], batch_size: int):
    return [data[i:i + batch_size] for i in range(0, len(data), batch_size)]


def doRewrite(rewriter: RewriterFromCSV):
    print(f"Rewriting")
    start_time = time.time()
    result = rewriter.readAndRewrite()
    end_time = time.time()
    print(f"Finished rewrite in {round((end_time - start_time) / 60, 2)} minutes")
    return result


def doRewriteWithTerms(rewriter: RewriterFromCSV, terms: list[str], alpha: float):
    print(f"Rewriting with terms {terms}")
    start_time = time.time()
    result = rewriter.readAndRewrite(alpha, terms)
    end_time = time.time()
    print(f"Finished rewrite with terms in {round((end_time - start_time) / 60, 2)} minutes")
    return result


# program entry
if __name__ == "__main__":
    # check parameters
    if len(sys.argv) < 3:
        print("Usage: python rewriterFromCSV.py <vocfile> <dataFile>")
    else:
        # read vocabulary file, then process csv data file
        if os.path.isfile(sys.argv[1]):
            # read file as vocabulary
            voc: Vocabulary = Vocabulary(sys.argv[1])
            if os.path.isfile(sys.argv[2]):

                terms = sys.argv[3:]
                alpha: float
                try:
                    alpha = float(terms[-1])
                    terms = terms[:-1]
                except ValueError:
                    alpha = 0.0
                except IndexError:
                    terms = None

                # both files are ok, process the data
                rw: RewriterFromCSV = RewriterFromCSV(voc, sys.argv[2])

                if terms is None:
                    R = doRewrite(rw)
                    toJson(beautifyFlights(R), SUMMARY_MEAN_PATH)
                else:
                    Rv = doRewriteWithTerms(rw, terms, alpha)
                    toJson(beautifyFlights(Rv), SUMMARY_ALPHA_PATH)

            else:
                print(f"Data file {sys.argv[2]} not found")
        else:
            print(f"Voc file {sys.argv[1]} not found")
