import sqlite3
from contextlib import contextmanager

from pkg_resources import resource_filename

from dss_generator.simulation import Simulation


class EntryExistsError(Exception):
    """
    Raised when trying to insert an entry that already exists in the database.
    """


class EntryNotFoundError(Exception):
    """
    Raised when trying to access an entry that does NOT exist in the database.
    """


class DeleteError(Exception):
    """
    Raised when trying to delete an entry that can not be deleted.
    """


class Connection:
    """
    An abstraction for a DB connection that provides specific methods to
    perform specific operations relevant to the simulations DB.

    All operations performed on a connection are only committed either when
    the commit() method is called or when the connection is closed.
    """

    def __init__(self, connection: sqlite3.Connection):
        self._connection = connection

        # Enable foreign keys because they are not enabled by default
        self._connection.execute('PRAGMA foreign_keys=ON')

        # Use a row factory to return the query results
        # This allows provides access to each column by name
        self._connection.row_factory = sqlite3.Row

    def insert_simulation(self, simulation: Simulation):
        """
        Inserts a new simulation in the database.

        :param simulation: simulation to insert
        :raise EntryExistsError: if the DB contains a simulation with the
        same ID of the simulation to be inserted
        """
        self._insert_in('simulation', **simulation.as_dict())

    def insert_in_queue(self, simulation_id: str, priority: int):
        """
        Inserts the simulation with the specified ID in the `queue` table.
        It associates the simulation with a priority value.

        To insert a simulation in the `queue` table it must have already been
        inserted in the DB.

        :param simulation_id: ID of the simulation to insert in `queue`
        :param priority:      priority value to assign the simulation
        :raise EntryNotFoundError: if the simulation does not exist
        :raise EntryExistsError: if a simulation with the same ID is already
        in the `queue` table
        """
        self._insert_in('queue',
                        id=simulation_id,
                        priority=priority)

    def execute_script(self, fp):
        """ Executes a script from an opened file """
        script = fp.read()
        self._connection.cursor().executescript(script)

    def commit(self):
        """ Commits the current operations """
        self._connection.commit()

    def rollback(self):
        """ Rolls back the operations performed since the last commit """
        self._connection.rollback()

    def close(self):
        """ Closes the connection. Afterwards, the connection cannot be used """
        self._connection.close()

    def _insert_in(self, table: str, **values):
        """
        Inserts an entry into a table with the specified name.

        :param table:  name of the table to insert entry to
        :param id:     ID to assign the entry
        :param values: extra values the entry includes
        :raise EntryExistsError: if the exists in the table
        :raise EntryNotFoundError: if a required entry does not exist in the
        database. This can be a simulation or a simulator.
        """
        try:
            keys = ",".join(key for key, _ in values.items())
            value_marks = ",".join("?" for i in values)
            values = list(value for _, value in values.items())

            query = f"INSERT INTO {table} ({keys}) VALUES ({value_marks})"
            self._connection.cursor().execute(query, values)

        except sqlite3.IntegrityError as error:

            if _is_foreign_key_constraint(error):
                raise EntryNotFoundError()
            elif _is_unique_constraint(error):
                raise EntryExistsError()
            else:
                # Just re-raise any other errors
                raise


class SimulationDB:
    """ Abstraction o access the simulations database """

    # Path to script used to create the DB tables
    CREATE_TABLES_SCRIPT = resource_filename(__name__, 'tables.sql')

    def __init__(self, db_file: str):
        """
        Initializes a new simulation DB.

        :param db_file: path to DB file
        """
        self._db_file = db_file

        # Create tables if necessary
        with open(self.CREATE_TABLES_SCRIPT) as file:
            with self.connect() as connection:
                connection.execute_script(file)

    @contextmanager
    def connect(self) -> Connection:
        """
        Connects to the database and yields a connection. It closes the
        connection when it is called to exit.
        """
        with sqlite3.connect(database=self._db_file) as connection:
            yield Connection(connection)


def _is_unique_constraint(error: sqlite3.IntegrityError):
    return 'UNIQUE constraint failed' in str(error)


def _is_foreign_key_constraint(error: sqlite3.IntegrityError):
    return 'FOREIGN KEY constraint failed' in str(error)
