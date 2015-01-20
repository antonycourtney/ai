import psycopg2
from psycopg2.extras import RealDictCursor

class UserDb:
  def __init__(self, dbParams):
    self.dbParams = dbParams
    self.conn = None

  def getConnection(self):
    if self.conn == None:
      self.conn = psycopg2.connect(host = self.dbParams['host'], port = self.dbParams['port'], 
            database = self.dbParams['db'], user = self.dbParams['user'], password = self.dbParams['password'])
    return self.conn

  def run_query(self, template, args):
    conn = self.getConnection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(template,args)
    res = cur.fetchall()
    cur.close()
    return res

  def run_insert_query(self, template, args):
    conn = self.getConnection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(template,args)
    conn.commit()
    cur.close()


  def get_user(self, userId):
    user_query = "select * from users where id = %s"
    return self.run_query(user_query,(userId,))

  def get_identities(self, userId):
    ids_query = "select * from identities where id = %s"
    return self.run_query(ids_query,(userId,))

  def log_download_failure(self, userId, messageId, except_msg, log_time):
    log_insert_query = "insert into download_failures values (%s,%s,%s,%s)"
    return self.run_insert_query(log_insert_query,(userId,messageId,except_msg,log_time))

  def set_create_table_time(self, userId, create_time):
    create_table_update_query = "update users set created_base_tables=%s where id=%s"
    return self.run_insert_query(create_table_update_query, (create_time, userId))
