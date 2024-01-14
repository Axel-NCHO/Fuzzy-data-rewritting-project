#!/usr/bin/python
# -*- coding: utf-8 -*-
from abc import ABC


class IFlightSummarizer(ABC):

    def addFlight(self, flight_rewritten: dict[str, float], *args):
        raise NotImplemented

    def getSummary(self):
        raise NotImplemented


class FlightSummarizerMean(IFlightSummarizer):

    def __init__(self, nb_flights: int):
        self.__nb_flights = nb_flights
        self.__flightsSummary: dict[str, float] = dict()

    def addFlight(self, flight_rewritten: dict[str, float], *args):
        for key, val in flight_rewritten.items():
            if key in self.__flightsSummary.keys():
                self.__flightsSummary[key] += val

            else:
                self.__flightsSummary[key] = val

    def getSummary(self):
        for key in self.__flightsSummary.keys():
            # normalise the value to get the mean
            self.__flightsSummary[key] /= self.__nb_flights
        return self.__flightsSummary


class FlightsSummarizerSatisfaction(IFlightSummarizer):

    def __init__(self, terms: list[str]):
        self.__nb_flights_satisfies_all_terms = 0
        self.__flightsSummary: dict[str, float] = dict()
        self.terms = terms

    def addFlight(self, flight_rewritten: dict[str, float], *args):
        satisfies_all_terms = all(flight_rewritten[term] == 1 for term in self.terms)
        if satisfies_all_terms:
            self.__nb_flights_satisfies_all_terms += 1
            for key, val in flight_rewritten.items():
                if key in self.__flightsSummary.keys():
                    self.__flightsSummary[key] += val
                else:
                    self.__flightsSummary[key] = val
        # for key, val in flight_rewritten.items():
        #     if key in self.terms:
        #         if key in self.__flightsSummary.keys():
        #             self.__flightsSummary[key] += val
        #         else:
        #             self.__flightsSummary[key] = val

    def getSummary(self):
        return self.__flightsSummary, self.__nb_flights_satisfies_all_terms

        # keys_to_delete = list()
        # for key in self.__flightsSummary.keys():
        #     normalised_val = self.__flightsSummary[key] / self.__nb_flights
        #     if normalised_val > self.alpha:
        #         self.__flightsSummary[key] = normalised_val
        #     else:
        #         keys_to_delete.append(key)
        # for key in keys_to_delete:
        #     del self.__flightsSummary[key]
        # return self.__flightsSummary
