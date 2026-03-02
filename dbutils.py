#
# BSD 3-Clause License
#
# Copyright (c) 2021-2023, Fred W6BSD
# All rights reserved.
# Edited by F4EGM 02/03/2026

import logging
import sqlite3
import time
from enum import Enum
from pathlib import Path
from threading import Thread

import geo
from lookup import DXEntity

logger = logging.getLogger(__name__)

SQL_TABLE = """
CREATE TABLE IF NOT EXISTS cqcalls
(
  call TEXT,
  extra TEXT,
  time TIMESTAMP,
  status INTEGER,
  snr INTEGER,
  grid TEXT,
  lat REAL,
  lon REAL,
  distance REAL,
  azimuth REAL,
  country TEXT,
  continent TEXT,
  cqzone INTEGER,
  ituzone INTEGER,
  frequency INTEGER,
  band INTEGER,
  packet JSON
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_call on cqcalls (call, band);
CREATE INDEX IF NOT EXISTS idx_time on cqcalls (time DESC);
CREATE INDEX IF NOT EXISTS idx_grid on cqcalls (grid ASC);
"""


class DBCommand(Enum):
  INSERT = 1
  STATUS = 2
  DELETE = 3


def connect_db(db_name):
  conn = sqlite3.connect(db_name, check_same_thread=False)
  conn.row_factory = sqlite3.Row
  return conn


def create_db(db_name):
  if isinstance(db_name, str):
    db_name = Path(db_name)
  if not db_name.parent.exists():
    db_name.parent.mkdir(parents=True)
  logger.info("Database: %s", db_name)
  with connect_db(db_name) as conn:
    curs = conn.cursor()
    curs.executescript(SQL_TABLE)


def get_call(db_name, call):
  req = "SELECT * FROM cqcalls WHERE call = ?"
  with connect_db(db_name) as conn:
    curs = conn.cursor()
    curs.execute(req, (call,))
    record = curs.fetchone()
  return dict(record) if record else {}


class DBInsert(Thread):

  # Refresh "time" on conflict: critical for delta-window selection.
  INSERT = """
  INSERT INTO cqcalls VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT(call, band) DO UPDATE SET
    extra     = excluded.extra,
    time      = excluded.time,
    snr       = excluded.snr,
    grid      = excluded.grid,
    lat       = excluded.lat,
    lon       = excluded.lon,
    distance  = excluded.distance,
    azimuth   = excluded.azimuth,
    country   = excluded.country,
    continent = excluded.continent,
    cqzone    = excluded.cqzone,
    ituzone   = excluded.ituzone,
    frequency = excluded.frequency,
    packet    = excluded.packet
  WHERE status <> 2
  """
  UPDATE = "UPDATE cqcalls SET status=? WHERE status <> 2 and call = ? and band = ?"
  DELETE = "DELETE from cqcalls WHERE status= 1 AND call = ? and band = ?"

  def __init__(self, db_name, queue, grid):
    super().__init__()
    self.db_name = db_name
    self.queue = queue
    self.origin = geo.grid2latlon(grid)
    self.dxe_lookup = DXEntity.DXCC().lookup

  def run(self):
    logger.info('Database Insert thread started')
    conn = connect_db(self.db_name)
    while True:
      cmd, data = self.queue.get()

      if cmd == DBCommand.INSERT:
        if data.get('grid'):
          lat, lon = geo.grid2latlon(data['grid'])
          data['lat'], data['lon'] = lat, lon
          data['distance'] = geo.distance(self.origin, (lat, lon))
          data['azimuth'] = geo.azimuth(self.origin, (lat, lon))
        else:
          # Tail-enders / replies may not contain a grid.
          data['grid'] = None
          data['lat'] = None
          data['lon'] = None
          data['distance'] = None
          data['azimuth'] = None

        try:
          dxentity = self.dxe_lookup(data['call'])
          data['country'] = dxentity.country
          data['continent'] = dxentity.continent
          data['cqzone'] = dxentity.cqzone
          data['ituzone'] = dxentity.ituzone
        except KeyError:
          logger.error('DXEntity for %s not found (fake callsign?)', data['call'])
          continue

        try:
          DBInsert.write(conn, data)
        except sqlite3.OperationalError as err:
          logger.warning("Queue len: %d - Error: %s", self.queue.qsize(), err)
        except AttributeError as err:
          logger.error(err)
          logger.error(data)

      elif cmd == DBCommand.STATUS:
        try:
          conn.execute(self.UPDATE, (data['status'], data['call'], data['band']))
          conn.commit()
        except sqlite3.OperationalError as err:
          logger.warning("Queue len: %d - Error: %s", self.queue.qsize(), err)

      elif cmd == DBCommand.DELETE:
        try:
          conn.execute(self.DELETE, (data['call'], data['band']))
          conn.commit()
        except sqlite3.OperationalError as err:
          logger.warning("Queue len: %d - Error: %s", self.queue.qsize(), err)
      else:
        logger.error("Unknown command: %s", cmd)

  @staticmethod
  def write(conn, data):
    req = (
      data.get('call'),
      data.get('extra'),
      data.get('packet', {}).get('Time'),
      0,
      data.get('packet', {}).get('SNR'),
      data.get('grid'),
      data.get('lat'),
      data.get('lon'),
      data.get('distance'),
      data.get('azimuth'),
      data.get('country'),
      data.get('continent'),
      data.get('cqzone'),
      data.get('ituzone'),
      data.get('frequency'),
      data.get('band'),
      data.get('packet'),
    )
    conn.execute(DBInsert.INSERT, req)
    conn.commit()


class Purge(Thread):

  PURGE = "DELETE from cqcalls where status <> 2 AND time < ?"
  INFO = "SELECT COUNT(*) as count FROM cqcalls"

  def __init__(self, db_name, retry_time):
    super().__init__()
    self.db_name = db_name
    self.retry_time = retry_time

  def run(self):
    logger.info('Database purge thread started')
    conn = connect_db(self.db_name)
    while True:
      now = time.time() - (self.retry_time * 60)
      now = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(now))
      try:
        conn.execute(self.PURGE, (now,))
        conn.commit()
      except sqlite3.OperationalError as err:
        logger.warning(err)
      result = conn.execute(self.INFO)
      logger.debug("Call list %d entries.", result.fetchone()['count'])
      time.sleep(60)


def get_band(freq):
  band = int(round(freq / 1000000))
  if band > 50:
    return 6
  if band == 24:
    return 12
  if band == 18:
    return 17
  if band == 14:
    return 20
  if band == 10:
    return 30
  if band == 7:
    return 40
  if band == 5:
    return 60
  if band == 3:
    return 80
  if band == 1:
    return 160
  return band
