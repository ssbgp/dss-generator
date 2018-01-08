import uuid
from typing import List

from dss_generator.simulation import Simulation
from dss_generator.topology import Topology


def read_topologies(topologies_path: str):
    """ Reads a topologies file and returns a list with those topologies """
    with open(topologies_path) as file:
        return [Topology(*line.strip().split("|")) for line in file if line]


def read_destinations(destinations_path: str) -> list:
    """ Reads a destinations file and returns a list with those destinations """
    with open(destinations_path) as file:
        return [int(line.strip()) for line in file if line]


def generate_simulations(topologies: List[Topology], destinations: List[int], repetitions: int,
                         min_delay: int, max_delay: int, threshold: int,
                         enable_reportnodes: bool) -> List[Simulation]:
    """
    Generates a simulation for each topology and each destination.

    :param topologies:          list containing the topologies
    :param destinations:        list containing the destinations
    :param repetitions:         number of repetitions for each simulation
    :param min_delay:           minimum message delay for each simulation
    :param max_delay:           maximum message delay for each simulation
    :param threshold:           threshold value for each simulation
    :param enable_reportnodes:  enable/disable report nodes feature
    :return: a list of generated simulations
    """
    simulations = []
    for topology in topologies:
        for destination in destinations:
            simulations.append(Simulation(
                topology=topology.name,
                destination=destination,
                repetitions=repetitions,
                min_delay=min_delay,
                max_delay=max_delay,
                threshold=threshold,
                stubs_file=topology.stubs,
                seed=None,
                enable_reportnodes=enable_reportnodes,
                id=str(uuid.uuid4())
            ))

    return simulations
