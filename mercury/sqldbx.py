#!/usr/bin/env python

#------ Database module --------

import sqlalchemy as sqla
import sqlalchemy.orm
from sqlalchemy.orm import mapper, scoped_session, sessionmaker, relation, relationship, clear_mappers
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy_utils import UUIDType
from sqlalchemy.schema import Sequence
import uuid

Base = declarative_base()

import types
import os
import sys
import time
from contextlib import contextmanager

DEFAULT_DB_CONNECT_RETRIES = 10


class NoSuchTableError(Exception):
    def __init__(self, tableName, schemaName):
        Exception.__init__(self, "No table named '%s' exists in database schema '%s'." % (tableName, schemaName))


class SQLDataTypeBuilder(object):
    def __init__(self, class_name, table_name, schema=None):
        self.name = class_name
        self.fields = []
        self.table_name = table_name
        self.schema = schema


    def add_primary_key_field(self, name, data_type, **kwargs):
        field = {}
        field['name'] = name
        if kwargs.get('sequence'):
            field['column'] = Column(data_type, Sequence(kwargs['sequence']), primary_key=True)
        else:
            field['column'] = Column(data_type, primary_key=True, **kwargs)
        self.fields.append(field)
        return self


    def add_foreign_key_field(self,
                              name,
                              data_type,
                              parent_table_name,
                              parent_table_pk_name):
        field = {}
        field['name'] = name
        fk_tokens = []
        if self.schema:
            fk_name = '%s.%s.%s' % (self.schema, parent_table_name, parent_table_pk_name)
        else:
            fk_name = '%s.%s' % (parent_table_name, parent_table_pk_name)

        field['column'] = Column(data_type, ForeignKey(fk_name))
        self.fields.append(field)
        return self


    def add_field(self, name, data_type, is_primary_key=False):
        field = {}
        field['name'] = name
        field['column'] = Column(data_type)
        self.fields.append(field)
        return self


    def add_relationship(self, relationship_name, related_type_name):
        field = {}
        field['name'] = relationship_name
        field['column'] = relationship(related_type_name)
        self.fields.append(field)
        return self


    def build(self):
        class_attrs = {}
        class_attrs['__tablename__'] = self.table_name
        if self.schema:
            class_attrs['__table_args__'] = {'schema': self.schema}

        for f in self.fields:
            class_attrs[f['name']] = f['column']
        klass = type(self.name, (Base,), class_attrs)
        return klass



class Database:
    """A wrapper around the basic SQLAlchemy DB connect logic.

    """

    def __init__(self, dbType, host, schema, port):
        """Create a Database instance ready for user login.

        Arguments:
        dbType -- for now, mysql and postgres only
        host -- host name or IP
        schema -- the database schema housing the desired tables
        """

        self.dbType = dbType
        self.host = host
        self.port = port
        self.schema = schema
        self.engine = None
        self.metadata = None
        self._session_factory = None


    def __create_url__(self, dbType, username, password):
        """Implement in subclasses to provide database-type-specific connection URLs."""
        pass


    def jdbc_url(self):
        """Return the connection URL without user credentials."""
        return 'jdbc:%s://%s:%s/%s' % (self.dbType, self.host, self.port, self.schema)


    def login(self, username, password, schema=None):
        """Connect as the specified user."""

        url = self.__create_url__(self.dbType, username, password)

        retries = 0
        connected = False
        while not connected and retries < DEFAULT_DB_CONNECT_RETRIES:
            try:
                self.engine = sqla.create_engine(url, echo=False)
                if schema:
                    self.metadata = sqla.MetaData(self.engine, schema=schema)
                else:
                    self.metadata = sqla.MetaData(self.engine)
                self.metadata.reflect(bind=self.engine)
                self._session_factory = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)
                connected = True
            except Exception as e:
                print(e)
                print(e.__class__.__name__)
                print(e.__dict__)
                time.sleep(1)
                retries += 1

        if not connected:
            raise Exception('Unable to connect to %s DB on host %s:%s.' % (self.dbType, self.host, str(self.port)))



    def get_metadata(self):
        return self.metadata

    def get_engine(self):
        return self.engine

    def get_session(self):
        return self._session_factory()

    def list_tables(self):
        return self.metadata.tables.keys()

    def get_table(self, name):
        """Passthrough call to SQLAlchemy reflection logic.

        Arguments:
        name -- The name of the table to retrieve. Must exist in the current schema.

        Returns:
        The requested table as an SQLAlchemy Table object.
        """

        if name not in self.metadata.tables:
            raise NoSuchTableError(name, self.schema)

        return self.metadata.tables[name]


@contextmanager
def txn_scope(database):
    session = database.get_session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()



class SQLServerDatabase(Database):
    '''Database type for connecting to Microsoft SQL Server instances
    '''

    def __init__(self, host, schema, port=1433):
        Database.__init__(self, 'mssql', host, schema, port)


    def __create_url__(self, dbtype, username, password):
        return '%s+pymssql://%s:%s@%s:%s/%s' % (self.dbType, username, password, self.host, self.port, self.schema)



class MySQLDatabase(Database):
    """A Database type for connecting to MySQL instances."""

    def __init__(self, host, schema, port=3306):
        Database.__init__(self, "mysql+mysqlconnector", host, schema, port)


    def __create_url__(self, dbType, username, password):
        return "%s://%s:%s@%s:%d/%s" % (self.dbType, username, password, self.host, self.port, self.schema)



class PostgreSQLDatabase(Database):
    """A Database type for connecting to PostgreSQL instances."""

    def __init__(self, host, schema, port=5432):
        Database.__init__(self, "postgresql+psycopg2", host, schema, port)


    def __create_url__(self, dbType, username, password):
        return "%s://%s:%s@%s:%d/%s" % (self.dbType, username, password, self.host, self.port, self.schema)


class RedshiftDatabase(Database):
    """A Database type for connecting to Redshift clusters."""
    def __init__(self, host, schema, port=5439):
        Database.__init__(self, "redshift+psycopg2", host, schema, port)

    def __create_url__(self, dbType, username, password):
        return "%s://%s:%s@%s:%d/%s" % (self.dbType, username, password, self.host, self.port, self.schema)


class NoSuchPluginError(Exception):
    def __init__(self, pluginName):
        Exception.__init__(self, "No plugin registered under the name '%s'." % pluginName)


class PluginMethodError(Exception):
    def __init__(self, pluginName, pluginMethodName):
        Exception.__init__(self, "The plugin registered as '%s' does not contain an execute() method." % (pluginName))


class PersistenceManager:
    """A logic center for database operations in a Serpentine app.

    Wraps SQLAlchemy lookup, insert/update, general querying, and O/R mapping facilities."""

    def __init__(self, database):
        self._typeMap = {}
        self.modelAliasMap = {}
        self.database = database
        self.metaData = self.database.get_metadata()
        self.pluginTable = {}
        self.mappers = {}


    def __del__(self):
        clear_mappers()

    def get_session(self):
        return self.database.get_session()

    def refresh_metadata(self):
        self.metaData = self.database.get_metadata()


    def load_table(self, tableName):
        """Retrieve table data using SQLAlchemy reflection"""

        return sqlalchemy.schema.Table(tableName, self.metaData, autoload = True)


    def str_to_class(self, objectTypeName):
        """A rudimentary class loader function.

        Arguments:
        objectTypeName -- a fully qualified name for the class to be loaded,
        in the form 'packagename.classname'.

        Returns:
        a Python Class object.
        """

        if objectTypeName.count('.') == 0:
            moduleName = __name__
            typeName = objectTypeName
        else:
            tokens = objectTypeName.rsplit('.', 1)
            moduleName = tokens[0]
            typeName = tokens[1]

        try:
            identifier = getattr(sys.modules[moduleName], typeName)
        except AttributeError:
            raise NameError("Class %s doesn't exist." % objectTypeName)
        if isinstance(identifier, (types.ClassType, types.TypeType)):
            return identifier
        raise TypeError("%s is not a class." % objectTypeName)

    def query(self, objectType, session):
        """A convenience function to create an SQLAlchemy Query object on the passed DB session.

        Arguments:
        objectType -- a Python class object, most likely returned from a call to str_to_class(...)

        Returns:
        An SQLAlchemy Query object, ready for data retrieval or further filtering. See SQLAlchemy docs
        for more information on Query objects.
        """

        return session.query(objectType)


    def map_type_to_table(self, modelClassName, tableName, **kwargs):
        """Call-through to SQLAlchemy O/R mapping routine. Creates an SQLAlchemy mapper instance.

        Arguments:
        modelClassName -- a fully-qualified class name (packagename.classname)
        tableName -- the name of the database table to be mapped to this class

        """

        dbTable = Table(tableName, self.metaData, autoload=True)
        objectType = self.str_to_class(modelClassName)
        if objectType not in self.mappers:
            self.mappers[objectType] = mapper(objectType, dbTable)

        if 'model_alias' in kwargs:
            modelAlias = kwargs['model_alias']
        else:
            modelAlias = modelClassName

        self.modelAliasMap[modelAlias] = modelClassName
        self._typeMap[modelClassName] = dbTable


    def map_parent_to_child(self, parentTypeName, parentTableName, parentTypeRefName, childTypeName, childTableName, childTypeRefName, **kwargs):
        """Create a parent-child (one to many relationship between two DB-mapped entities in SQLAlchemy's O/R mapping layer.

        Arguments:

        Returns:
        """

        parentTable = Table(parentTableName, self.metaData, autoload=True)
        parentObjectType = self.str_to_class(parentTypeName)

        childTable = Table(childTableName, self.metaData, autoload=True)
        childObjectType = self.str_to_class(childTypeName)

        if childObjectType not in self.mappers:
            self.mappers[childObjectType] = mapper(childObjectType, childTable)

        self.mappers[parentObjectType] = mapper(parentObjectType, parentTable, properties={
                childTypeRefName : relation(childObjectType, backref = parentTypeRefName)})

        parentAlias = kwargs['parent_model_alias']
        childAlias = kwargs['child_model_alias']

        self.mapTypeToTable(parentTypeName, parentTable.name, model_alias = parentAlias)
        self.mapTypeToTable(childTypeName, childTable.name, model_alias = childAlias)



    def map_peer_to_peer(self, parentTypeName, parentTableName, parentTypeRefName, peerTypeName, peerTableName, peerTypeRefName, **kwargs):
        """Create a peer-peer (one to one) relationship between two DB-mapped entities in SQLAlchemy's O/R mapping layer.

        Arguments:

        Returns:
        """

        parentTable = Table(parentTableName, self.metaData, autoload=True)
        parentObjectType = self.str_to_class(parentTypeName)

        peerTable = Table(peerTableName, self.metaData, autoload=True)
        peerObjectType = self.str_to_class(peerTypeName)

        if peerObjectType not in self.mappers:
            self.mappers[peerObjectType] = mapper(peerObjectType, peerTable, non_primary=True)

        self.mappers[parentObjectType] = mapper(parentObjectType, parentTable, properties={
                peerTypeRefName : relation(peerObjectType, backref = parentTypeRefName, uselist = False), })

        parentAlias = kwargs['model_alias']
        peerAlias = kwargs['peer_model_alias']

        self.mapTypeToTable(parentTypeName, parentTable.name, model_alias = parentAlias)
        self.mapTypeToTable(peerTypeName, peerTable.name, model_alias = peerAlias)



    def get_table_for_type(self, modelName):
        if modelName not in self.modelAliasMap:
            raise NoTypeMappingError(modelName)

        return self._typeMap[self.modelAliasMap[modelName]]

    def retrieve_all(self, objectTypeName, session):
        objClass = self.str_to_class(objectTypeName)
        resultSet = session.query(objClass).all()
        return resultSet

    def insert(self, object, session):
        session.add(object)

    def update(self, object, session):
        session.flush()

    def delete(self, object, session):
        session.delete(object)

    def load_by_key(self, objectTypeName, objectID, session):
        query = session.query(self.str_to_class(objectTypeName)).filter_by(id = objectID)
        return query.first()

    def register_plugin(self, plugin, name):
        self.pluginTable[name] = plugin


    def call_plugin(self, pluginName, targetObject):
        plugin = self.pluginTable[pluginName]
        if plugin == None:
            raise NoSuchPluginError(pluginName)

        try:
            return plugin.performOperation(self, targetObject)
        except AttributeError as err:
            raise PluginMethodError(pluginName, 'execute')


    class DBTask(object):
        def __init__(self, name='anonymous DB task'):
            self.name = name
            self._id = None
    
        def _get_uuid(self):
            # can be overridden in descendant classes
            return uuid.uuid4()
    
        def __str__(self):
            return 'dbtask[%s]:%s' % (self.name, self.uuid)
    
        @property
        def uuid(self):
            if not self._id:
                self._id = self._get_uuid()
            return self._id
        

class ConnectionPoolManager(object):
    def __init__(self, hostname, db_name, username, password, port='5432'):
        #self.connect_string = 'postgresql://'+user+':'+password+'@'+host+':'+port+'/'+dbname
        #conn = create_engine(connect_string, pool_size=20, max_overflow_size=0)
        self.hostname = hostname
        self.dbname = db_name
        self.username = username
        self.password = password
        self.port = port
        self.connection_pool = pool.QueuePool(self._getconn,
                                             max_overflow=10,
                                             pool_size=5)
        self.connection_table = {}
        
    
    def _get_connection(self): # can override in subclasses
        c = psycopg2.connect(username=self.username,
                            host=self.hostname,
                            dbname=self.dbname,
                            password=self.password,
                            port=self.port)
        return c

    
    def get_connection(self, db_task):
        conn = self._get_connection()
        self.connection_table[db_task.uuid()] = conn
        return conn
    

@contextmanager
def get_connection(connection_pool_mgr, db_task):    
    conn = connection_mgr.get_connection(db_task)
    yield conn
    connection_mgr.close_connection(db_task)
