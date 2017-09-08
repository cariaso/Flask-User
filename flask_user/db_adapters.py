""" This file shields Flask-User code from database/ORM specific functions.

    :copyright: (c) 2013 by Ling Thio
    :author: Ling Thio (ling.thio@gmail.com)
    :license: Simplified BSD License, see LICENSE.txt for more details."""

from __future__ import print_function
from datetime import datetime
from flask_login import current_user

class DBAdapter(object):
    """ This object is used to shield Flask-User from ORM specific functions.
        It's used as the base class for ORM specific adapters like SQLAlchemyAdapter."""
    def __init__(self, db, UserClass, UserAuthClass=None, UserEmailClass=None, UserProfileClass=None, UserInvitationClass=None):
        self.db = db
        self.UserClass = UserClass                  # first_name, last_name, etc.
        self.UserAuthClass = UserAuthClass          # username, password, etc.
        self.UserEmailClass = UserEmailClass        # For multiple emails per user
        self.UserProfileClass = UserProfileClass    # Distinguish between v0.5 or v0.6 call
        self.UserInvitationClass = UserInvitationClass

        if UserProfileClass:    # pragma: no cover
            # Print deprecation warning
            print('Warning: The UserProfileClass parameter in DBAdapter() will be deprecated\n'+
                  'in the future. Use "UserAuthClass" and "UserClass" parameters instead.\n'+
                  'See http://pythonhosted.org/Flask-User/data_models.html.')
            # Ensure backward compatibility with v0.5 code
            self.UserAuthClass = UserClass
            self.UserClass = UserProfileClass



class SQLAlchemyAdapter(DBAdapter):
    """ This object is used to shield Flask-User from SQLAlchemy specific functions."""
    def __init__(self, db, UserClass, UserProfileClass=None, UserAuthClass=None, UserEmailClass=None, UserInvitationClass=None):
        super(SQLAlchemyAdapter, self).__init__(db, UserClass, UserAuthClass, UserEmailClass, UserProfileClass, UserInvitationClass)

    def get_object(self, ObjectClass, id):
        """ Retrieve one object specified by the primary key 'pk' """
        return ObjectClass.query.get(id)

    def find_all_objects(self, ObjectClass, **kwargs):
        """ Retrieve all objects matching the case sensitive filters in 'kwargs'. """

        # Convert each name/value pair in 'kwargs' into a filter
        query = ObjectClass.query
        for field_name, field_value in kwargs.items():

            # Make sure that ObjectClass has a 'field_name' property
            field = getattr(ObjectClass, field_name, None)
            if field is None:
                raise KeyError("SQLAlchemyAdapter.find_first_object(): Class '%s' has no field '%s'." % (ObjectClass, field_name))

            # Add a filter to the query
            query = query.filter(field.in_((field_value,)))

        # Execute query
        return query.all()


    def find_first_object(self, ObjectClass, **kwargs):
        """ Retrieve the first object matching the case sensitive filters in 'kwargs'. """

        # Convert each name/value pair in 'kwargs' into a filter
        query = ObjectClass.query
        for field_name, field_value in kwargs.items():

            # Make sure that ObjectClass has a 'field_name' property
            field = getattr(ObjectClass, field_name, None)
            if field is None:
                raise KeyError("SQLAlchemyAdapter.find_first_object(): Class '%s' has no field '%s'." % (ObjectClass, field_name))

            # Add a case sensitive filter to the query
            query = query.filter(field==field_value)  # case sensitive!!

        # Execute query
        return query.first()

    def ifind_first_object(self, ObjectClass, **kwargs):
        """ Retrieve the first object matching the case insensitive filters in 'kwargs'. """

        # Convert each name/value pair in 'kwargs' into a filter
        query = ObjectClass.query
        for field_name, field_value in kwargs.items():

            # Make sure that ObjectClass has a 'field_name' property
            field = getattr(ObjectClass, field_name, None)
            if field is None:
                raise KeyError("SQLAlchemyAdapter.find_first_object(): Class '%s' has no field '%s'." % (ObjectClass, field_name))

            # Add a case sensitive filter to the query
            query = query.filter(field.ilike(field_value))  # case INsensitive!!

        # Execute query
        return query.first()

    def add_object(self, ObjectClass, **kwargs):
        """ Add an object of class 'ObjectClass' with fields and values specified in '**kwargs'. """
        object=ObjectClass(**kwargs)
        self.db.session.add(object)
        return object

    def update_object(self, object, **kwargs):
        """ Update object 'object' with the fields and values specified in '**kwargs'. """
        for key,value in kwargs.items():
            if hasattr(object, key):
                setattr(object, key, value)
            else:
                raise KeyError("Object '%s' has no field '%s'." % (type(object), key))

    def delete_object(self, object):
        """ Delete object 'object'. """
        self.db.session.delete(object)

    def commit(self):
        self.db.session.commit()

import pdb
from flywheel import Model, Field, Engine

class DynamoDBAdapter(DBAdapter):
    """ This object is used to shield Flask-User from Dynamo specific functions."""
    def __init__(self, db, UserClass, UserProfileClass=None, UserAuthClass=None, UserEmailClass=None, UserInvitationClass=None):
        self.db = db
        self.UserClass = UserClass
        self.UserProfileClass = UserProfileClass
        self.UserAuthClass = UserAuthClass
        self.UserEmailClass = UserEmailClass
        self.UserInvitationClass = UserInvitationClass
        #super(SQLAlchemyAdapter, self).__init__(db, UserClass, UserAuthClass, UserEmailClass, UserProfileClass, UserInvitationClass)

    def get_object(self, ObjectClass, id):
        """ Retrieve one object specified by the primary key 'pk' """
        print('dynamo.get(%s, %s)' % (ObjectClass, str(id)))
        pdb.set_trace()
        out = self.db.engine.get(id)
        return out
        return ObjectClass.query.get(id)

    def find_all_objects(self, ObjectClass, **kwargs):
        """ Retrieve all objects matching the case sensitive filters in 'kwargs'. """

        print('dynamo.find_first_object(%s, %s)' % (ObjectClass, str(kwargs)))

        query = self.db.engine.query(ObjectClass)
        for field_name, field_value in kwargs.items():

            # Make sure that ObjectClass has a 'field_name' property
            field = getattr(ObjectClass, field_name, None)
            if field is None:
                raise KeyError("DynamoDBAdapter.find_first_object(): Class '%s' has no field '%s'." % (ObjectClass, field_name))

            # Add a case sensitive filter to the query
            query = query.filter(field == field_value)

        # Execute query
        return query.all(desc=True)

    def find_first_object(self, ObjectClass, **kwargs):
        """ Retrieve the first object matching the case sensitive filters in 'kwargs'. """

        print('dynamo.find_first_object(%s, %s)' % (ObjectClass, str(kwargs)))

        query = self.db.engine.query(ObjectClass)
        for field_name, field_value in kwargs.items():

            # Make sure that ObjectClass has a 'field_name' property
            field = getattr(ObjectClass, field_name, None)
            if field is None:
                raise KeyError("DynamoDBAdapter.find_first_object(): Class '%s' has no field '%s'." % (ObjectClass, field_name))

            # Add a case sensitive filter to the query
            query = query.filter(field == field_value)

        # Execute query
        return query.first(desc=True)

    def ifind_first_object(self, ObjectClass, **kwargs):
        """ Retrieve the first object matching the case insensitive filters in 'kwargs'. """

        # Convert each name/value pair in 'kwargs' into a filter
        print("dynamo.ifind_first_object(%s, %s). I don't actually support case insensitive yet" % (ObjectClass, str(kwargs)))

        query = self.db.engine.query(ObjectClass)
        for field_name, field_value in kwargs.items():

            # Make sure that ObjectClass has a 'field_name' property
            field = getattr(ObjectClass, field_name, None)
            if field is None:
                raise KeyError("DynamoDBAdapter.ifind_first_object(): Class '%s' has no field '%s'." % (ObjectClass, field_name))

            # Add a case sensitive filter to the query
            query = query.filter(field == field_value)

        # Execute query
        return query.first(desc=True)

    def add_object(self, ObjectClass, **kwargs):
        """ Add an object of class 'ObjectClass' with fields and values specified in '**kwargs'. """
        print('dynamo.add_object(%s, %s)' % (ObjectClass, str(kwargs)))
        object=ObjectClass(**kwargs)
        self.db.engine.save(object)
        return object

    def update_object(self, object, **kwargs):
        """ Update object 'object' with the fields and values specified in '**kwargs'. """
        pdb.set_trace()
        for key,value in kwargs.items():
            if hasattr(object, key):
                setattr(object, key, value)
            else:
                raise KeyError("Object '%s' has no field '%s'." % (type(object), key))

    def delete_object(self, object):
        """ Delete object 'object'. """
        #pdb.set_trace()
        self.db.engine.delete_key(object)#, userid='abc123', id='1')
        print('dynamo.delete_object(%s)' % object)
        #self.db.session.delete(object)

    def commit(self):
        #pdb.set_trace()
        print('dynamo.commit()')
        self.db.engine.sync()
        #self.db.session.commit()
        
        
