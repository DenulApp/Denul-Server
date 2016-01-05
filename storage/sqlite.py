# -*- encoding: utf-8 -*-

import sqlite3


class SqliteBackend():
    SCHEMA_VERSION = 2
    """The SQLite storage backend of the server application.

    Data is saved into and read from a local SQLite database.
    """

    def __init__(self, dbname="denul.db"):
        """Initialize the DB backend.

        Open the SQLite database and perform sanity checks.

        Keywork arguments:
        dbname -- Name of the DB file. (default: denul.db)
        """
        self.conn = sqlite3.connect(dbname)

        # Enable foreign key support
        c = self.conn.cursor()
        c.execute("PRAGMA FOREIGN_KEYS = ON;")

        # Validate layout
        self._validate_layout()

    def _validate_layout(self):
        """Validate if the format of the database is sane"""
        # Get a cursor object
        c = self.conn.cursor()

        # Check the user_version pragma
        c.execute("PRAGMA user_version")
        uv = c.fetchone()
        if uv[0] is 0:
            # No tables exist
            self._create_layout()
        elif uv[0] != self.SCHEMA_VERSION:
            # Different version, upgrade
            self._upgrade(uv[0], self.SCHEMA_VERSION)

    def _create_layout(self):
        """If a new database has been opened, create the table layout"""
        # Get a cursor
        c = self.conn.cursor()

        # Create table for key-value-pairs
        c.execute("CREATE TABLE kv (key blob, value blob);")
        c.execute("CREATE TABLE study (id INTEGER PRIMARY KEY, ident BLOB, pubkey BLOB, message BLOB);")
        c.execute("CREATE TABLE studyEntry (id INTEGER PRIMARY KEY, study INTEGER, data BLOB, FOREIGN KEY (study) REFERENCES study(id) ON DELETE CASCADE);")
        # Set the user_version pragma to indicate the version of the DB layout
        c.execute("PRAGMA user_version = 2")

        # Commit transaction
        self.conn.commit()

    def _upgrade(self, old, new):
        """Upgrade the database scheme"""
        c = self.conn.cursor()

        if old == 1 and new == 2:
            # Add new tables
            c.execute("CREATE TABLE study (id INTEGER PRIMARY KEY, ident BLOB, pubkey BLOB, message BLOB);")
            c.execute("CREATE TABLE studyEntry (id INTEGER PRIMARY KEY, study INTEGER, data BLOB, FOREIGN KEY (study) REFERENCES study(id) ON DELETE CASCADE);")
            c.execute("PRAGMA user_version = 2;")
        else:
            print "Unknown database upgrade path:", old, "to", new

    def insert_kv(self, key, value):
        """Insert a key-value-pair into the database

        Keyword arguments:
        key   -- Key under which the value should be stored
        value -- Value that should be stored
        """
        # Get a cursor
        c = self.conn.cursor()

        # Check if something already exists under that key
        c.execute("SELECT * FROM kv WHERE key = ?", (sqlite3.Binary(key), ))
        if c.fetchone() is not None:
            raise KeyError("Key already in use")

        # Perform the insertion
        c.execute("INSERT INTO kv VALUES (?, ?)", (sqlite3.Binary(key), sqlite3.Binary(value)))
        # Commit transaction
        self.conn.commit()

    def query_kv(self, key):
        """Query the database for the value associated with a key

        Keyword arguments:
        key -- The key that should be queried

        Returns the value, or None if the key has no associated value.
        """
        # Get a cursor
        c = self.conn.cursor()

        # Retrieve value from database
        c.execute("SELECT value FROM kv WHERE key = ?", (sqlite3.Binary(key), ))

        # Return the result
        try:
            return c.fetchone()[0]
        except TypeError:  # Thrown if no result is in the database
            return None

    def delete_kv(self, key):
        """Delete the key-value-pair associated with the provided key

        Keyword arguments:
        key -- The key that should be deleted

        Returns True if the pair has been deleted, False if no such pair
        existed.
        """
        # Check if the key exists
        if self.query_kv(key) is None:
            return False

        # Get a cursor
        c = self.conn.cursor()

        # Perform deletion
        c.execute("DELETE FROM kv WHERE key = ?", (sqlite3.Binary(key), ))
        # Commit transaction
        self.conn.commit()

        # Return success
        return True

    def all_keys(self):
        """Read all keys from the database and return them as a list."""
        # Get a cursor
        c = self.conn.cursor()

        # Retrieve all keys
        c.execute("SELECT key FROM kv")

        # return result
        return c.fetchall()

    def insert_study(self, ident, pubkey, msg):
        """Insert a new study into the database"""
        # Get a cursor
        c = self.conn.cursor()

        # Run insert
        c.execute("INSERT INTO study (ident, pubkey, message) VALUES (?, ?, ?)",
                  (sqlite3.Binary(ident), sqlite3.Binary(pubkey),
                   sqlite3.Binary(msg.SerializeToString())))
        # Commit
        self.conn.commit()

    def list_studies(self):
        """Get a List of Studies in the database"""
        # Get a cursor
        c = self.conn.cursor()
        # Run query
        c.execute("SELECT message FROM study;")
        # Return raw result (needs to be parsed into messages by caller)
        return c.fetchall()

    def insert_studyjoin(self, ident, data):
        """Insert a studyJoin message into the database"""
        # Get a cursor
        c = self.conn.cursor()

        # Determine Database ID of study
        c.execute("SELECT id FROM study WHERE ident LIKE ?;",
                  (sqlite3.Binary(ident), ))
        # Fetch result
        try:
            pkey = c.fetchone()[0]
        except (TypeError, IndexError):
            # Something went wrong while retrieving the primary key
            # No such study?
            return False

        # Run insert
        c.execute("INSERT INTO studyEntry (study, data) VALUES (?, ?);",
                  (pkey, sqlite3.Binary(data)))
        # Commit
        self.conn.commit()
        # Indicate success
        return True

    def query_study(self, ident):
        # Get a cursor
        c = self.conn.cursor()
        # Determine database ID
        c.execute("SELECT id FROM study WHERE ident LIKE ?;",
                  (sqlite3.Binary(ident)))
        try:
            ident = c.fetchone()[0]
        except (IndexError, TypeError):
            return []
        # Read all data blocks related to that study from the DB
        c.execute("SELECT data FROM studyEntry WHERE study LIKE ?;",
                  (ident, ))
        rv = c.fetchall()
        # Delete all database entries related to that study
        c.execute("DELETE FROM studyEntry WHERE study LIKE ?;",
                  (ident, ))
        # Return
        return rv

    def query_study_pkey(self, ident):
        # Get a cursor
        c = self.conn.cursor()
        # Run query
        c.execute("SELECT pubkey FROM study WHERE ident LIKE ?;",
                  (sqlite3.Binary(ident), ))
        # Return result
        try:
            return c.fetchone()[0]
        except (TypeError, IndexError):
            return None

    def delete_study(self, ident):
        # Get a cursor
        c = self.conn.cursor()
        # Run query
        c.execute("DELETE FROM study WHERE ident LIKE ?;",
                  (sqlite3.Binary(ident), ))
        return c.rowcount == 1

    def close(self):
        """Close the database connection"""
        self.conn.close()
