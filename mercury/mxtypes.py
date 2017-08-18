#!/usr/bin/env python


from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy_utils import UUIDType
import uuid

Base = declarative_base()


class Asset(Base):
    __tablename__ = 'assets'
    id = Column(UUIDType(binary=False), primary_key=True)
    schedule_id = Column(UUIDType(binary=False))
    file_validation_profile_id = Column(UUIDType(binary=False))
    asset_group_id = Column(UUIDType(binary=False))
    filename = Column(String())
    filename_type = Column(Integer())
    filename_prefix = Column(String())


class Schedule(Base):
    __tablename__ = 'schedules'
    __table_args__ = {'schema': 'ingest'}
    id = Column(UUIDType(binary=False), primary_key=True)
    client_id = Column(UUIDType(binary=False))
    name = Column(String())
    rcv_interval = Column(Integer())
    rcv_interval_unit = Column(Integer())
