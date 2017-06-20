#!/usr/bin/env python


import json
import os
import docopt
import httplib
import datetime


date_format = '%Y-%m-%d'
export_retries = 5



MIXPANEL_NAME_DELIMITER=':'



class MalformedSignalException(Exception):
    def __init__(self, sig_name, sig_time, sig_id, sig_props):
        Exception.__init__(self, 'Signal ID %s contains missing or invalid data: [name]: %s, [timestamp]: %s, [properties]: %s' % (sig_id, sig_name, sig_time, sig_props))
        


        
class Signal(object):
    def __init__(self, name, timestamp, id, json_props):
        self.name = name
        self.timestamp = timestamp
        self.id = id
        self.properties = json_props
        self.channel = None
        self.subchannel = None
        self.action = None

        tokens = [t.strip() for t in self.name.split(MIXPANEL_NAME_DELIMITER)]
        if len(tokens) < 2:
            self.channel = tokens[0]
            self.action = tokens[0]
        elif len(tokens) == 2:
            self.channel = tokens[0]
            self.action = tokens[1]
        elif len(tokens) >= 3:
            self.channel = tokens[0]
            self.subchannel = tokens[1]
            self.action = tokens[2]        
        
        
            
            
class Event(object):
    def __init__(self, type, **data):
        self.type = type
        for key, value in data.iteritems():
            setattr(self, key, value)
        


class EventFactory(object):
    def create_event(self, event_type_name, signal):
        
        raise NotImplementedException(self.__class__.__name__)
            

    
class Sensor(EventFactory):
    
    def read(self, signal):
        raise NotImplementedException(self.__class__.__name__)


    
class SessionSensor(Sensor):

    def create_event(self):
        pass
    
    def read(self, signal):
        pass
        



    
class SignalProcessor(object):
    def __init__(self):
        self.sensors = {}


    def register_sensor(self, sensor, event_type_name):
        self.sensors[event_type_name] = sensor


    def process(self, signal_queue, event_queue):
        pass
    


