from typing import Iterator

from dss_generator.database import SimulationDB
from dss_generator.simulation import Simulation


class SimulationQueue:
    """ Abstraction for the simulation queue """

    def __init__(self, database: SimulationDB):
        self._database = database

    def add(self, simulation: Simulation, priority: int):
        """ Adds a *simulation* to the queue with the given *priority* """
        with self._database.connect() as db:
            db.insert_simulation(simulation)
            db.insert_in_queue(simulation.id, priority)

    def add_all(self, simulations: Iterator[Simulation], priority: int):
        """ Adds multiple *simulations* to the queue with the given *priority* """
        with self._database.connect() as db:
            for simulation in simulations:
                db.insert_simulation(simulation)
                db.insert_in_queue(simulation.id, priority)
