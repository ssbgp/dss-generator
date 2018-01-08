"""
SSBGP-DSS Simulations

Usage:
  dss-generator <topologies> <destinations> <priority> [--reportnodes] [--c=<repetitions>]
                [--min=<min_delay>] [--max=<max_delay>] [--th=<threshold>] [ --db=<db_path>]
  dss-generator (-h | --help)

Options:
  -h --help          Show this screen.
  --version          Show version.
  --c=<repetitions>  Number of repetitions [default: 100].
  --min=<min_delay>  Minimum message delay [default: 10].
  --max=<max_delay>  Maximum message delay [default: 1000].
  --th=<threshold>   Threshold value [default: 2000000].
  --db=<db_path>     Path to the DB file [default: simulations.db].
  --reportnodes      Enable report nodes data individually.

"""

from docopt import docopt

from dss_generator.database import SimulationDB
from dss_generator.generate_simulations import read_destinations, read_topologies, \
    generate_simulations
from dss_generator.sim_queue import SimulationQueue


def main():
    args = docopt(__doc__)

    priority = int(args['<priority>'])
    db_path = args['--db']

    topologies = read_topologies(args['<topologies>'])
    destinations = read_destinations(args['<destinations>'])

    print(f"Found {len(topologies)} topologies and {len(destinations)} destinations")
    print("Generating simulations...")
    simulations = generate_simulations(
        topologies=topologies,
        destinations=destinations,
        repetitions=int(args['--c']),
        min_delay=int(args['--min']),
        max_delay=int(args['--max']),
        threshold=int(args['--th']),
        enable_reportnodes=args['--reportnodes']
    )

    queue = SimulationQueue(SimulationDB(db_path))
    queue.add_all(simulations, priority)

    print(f"Done! {len(simulations)} simulations were added with priority {priority}")


if __name__ == '__main__':
    main()
