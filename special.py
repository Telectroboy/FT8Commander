#
# BSD 3-Clause License
#
# Copyright (c) 2026, Jean-Philippe F4EGM
# All rights reserved.
#

from dbutils import connect_db
from .base import CallSelector


class DXCC100(CallSelector):

  WORKED = ("SELECT country FROM cqcalls WHERE status = 2 and band = ? "
            "GROUP BY country HAVING count(*) >= ?")

  def __init__(self):
    super().__init__()
    self.worked_count = getattr(self.config, "worked_count", 2)

  def get(self, band):
    records = []
    with connect_db(self.db_name) as conn:
      curs = conn.cursor()
      result = curs.execute(self.WORKED, (band, self.worked_count,))
      worked = set(r['country'] for r in result)

    for record in super().get(band):
      if record['country'] not in worked:
        records.append(record)

    return self.select_record(records)


class Extra(CallSelector):

  def __init__(self):
    super().__init__()
    self.reverse = getattr(self.config, 'reverse', False)
    self.ex_list = set(getattr(self.config, 'list', []))

  def get(self, band):
    records = []
    for record in super().get(band):
      if (record['extra'] in self.ex_list) ^ self.reverse:
        records.append(record)
    return self.select_record(records)

  @staticmethod
  def sort(records):
    # Tail-enders: transcontinental first (continent != EU), then farther (if known), then SNR.
    def key(r):
      cont = r.get('continent')
      trans = 1 if (cont and cont != 'EU') else 0
      dist = r.get('distance')
      if dist is None:
        dist = -1
      snr = r.get('snr', -999)
      return (trans, dist, snr)
    return sorted(records, key=key, reverse=True)
