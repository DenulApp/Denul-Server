# -*- encoding: utf-8 -*-

import sqlite3


class SqliteBackend():
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
        if not self._validate_layout():
            self._create_layout()

    def _validate_layout(self):
        """Validate if the format of the database is sane"""
        # Get a cursor object
        c = self.conn.cursor()

        # Check the user_version pragma
        c.execute("PRAGMA user_version")
        uv = c.fetchone()
        if uv is None or uv[0] != 1:
            return False
        else:
            return True
        # I am aware that this is a horrible way to check this.
        # TODO I will implement a decent check later

    def _create_layout(self):
        """If a new database has been opened, create the table layout"""
        # Get a cursor
        c = self.conn.cursor()

        # Create table for key-value-pairs
        c.execute("CREATE TABLE kv (key blob, value blob)")
        # Set the user_version pragma to indicate the version of the DB layout
        c.execute("PRAGMA user_version = 1")

        # Commit transaction
        self.conn.commit()

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

    def close(self):
        """Close the database connection"""
        self.conn.close()
