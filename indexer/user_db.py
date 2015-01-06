import psycopg2
from psycopg2.extras import RealDictCursor

class UserDb:
  def __init__(self, dbParams):
    self.dbParams = dbParams
    self.conn = None

  def getConnection(self):
    print "getConnection: ", self.dbParams
    if self.conn == None:
      self.conn = psycopg2.connect(host = self.dbParams['host'], port = self.dbParams['port'], 
            database = self.dbParams['db'], user = self.dbParams['user'], password = self.dbParams['password'])
    return self.conn

  def run_query(self, query, userId):
    conn = self.getConnection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query, (userId,))
    res = cur.fetchall()
    return res

  def get_user(self, userId):
    user_query = "select * from users where id = %s"
    return self.run_query(user_query,userId)

  def get_identities(self, userId):
    ids_query = "select * from identities where id = %s"
    return self.run_query(ids_query,userId)
