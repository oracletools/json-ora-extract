#! /batch/Bic/oats/py27/bin/python
"""
USAGE:
	./json_ora_extract.py  -g scott@tiger:1521/ORA12 -s in_sql.sql -o out_json.json -a 10000 -c 0

"""
import os, sys
import json
import atexit
import datetime
import cx_Oracle
import traceback
import gzip
from optparse import OptionParser
from binascii import hexlify, unhexlify
from simplecrypt import  decrypt
import pprint as pp
from pprint import pprint
SUCCESS=0
FAILED=1
DEFAULT_SQL_FILE	=	'in_sql.sql'
DEFAULT_JSON_FILE	=	'out_json.json'
DEFAULT_ARRAY_SIZE	=	1000
DEFAULT_COMPRESS 	=	1
exit_status=FAILED
job_status_file=os.path.basename(__file__)+'.status'
home= os.path.abspath(os.path.dirname(sys.argv[0]))
def formatExceptionInfo(maxTBlevel=5):
	cla, exc, trbk = sys.exc_info()
	excName = cla.__name__
	try:
		excArgs = exc.__dict__["args"]
	except KeyError:
		excArgs = "<no args>"
	excTb = traceback.format_tb(trbk, maxTBlevel)
	return (excName, excArgs, excTb)

def decrypt_ora_password(cypher):
	secret = 'OATS'
	cyphertext=unhexlify(cypher)
	plaintext = decrypt(secret, cyphertext)
	return plaintext.decode()
	
def chunks(cur): # 65536
	while True:
		rows=cur.fetchmany()
		if not rows: break;
		yield rows
		
def save_status():
	global job_status_file, exit_status, opt, d

	p = pp.PrettyPrinter(indent=4)
	with open(job_status_file, "w") as py_file:			
		py_file.write('status=%s' % (p.pformat(exit_status)))
def datetime_handler(x):
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    raise TypeError("Unknown type")

def rows_to_dict_list(chunk,cur):
    columns = [i[0] for i in cur.description]
    return [dict(zip(columns, row)) for row in chunk]

if __name__ == "__main__":
	atexit.register(save_status)
	parser = OptionParser()
	parser.add_option("-g", "--db_login", dest="db_login", type=str, default='scott@tiger:1521/ORA12')
	parser.add_option("-s", "--in_sql_file", dest="in_sql_file", type=str, default=DEFAULT_SQL_FILE)
	parser.add_option("-o", "--out_json_file", dest="out_json_file", type=str, default=DEFAULT_JSON_FILE)
	parser.add_option("-a", "--array_size", dest="array_size", type=int, default=DEFAULT_ARRAY_SIZE)
	parser.add_option("-c", "--compress", dest="compress", type=int, default=DEFAULT_COMPRESS)
	
	
	(opt, args) = parser.parse_args()
	if len(sys.argv)==1:
		print(__doc__)
		sys.exit(FAILED)
		
	pp.pprint(opt)
	if 1:
		connector= opt.db_login
		dbuser, s = connector.split('@')
		ip, s = s.split(':')
		port, service_name= s.split('/')
		try:
			dsn = cx_Oracle.makedsn(ip, port, service_name=service_name)
			with open(os.path.join(home,'cypher.txt'), 'rb') as fh:
				c= fh.read()

			secret = decrypt_ora_password(c)
			con = cx_Oracle.connect(dbuser, secret, dsn,threaded=True)
			conninfo =(dbuser,ip, port, service_name, secret)
			#pprint(conninfo)
			cur = con.cursor()
			cur.arraysize=opt.array_size
			sql_file=opt.in_sql_file
			assert os.path.isfile(sql_file)
			sel=None
			with open(sql_file, 'r') as fh:
				sel = fh.read().strip().strip(';')
			assert sel
			cur.execute(sel)
			compress=opt.compress
			if compress:
				fn=opt.out_json_file
				fn='%s.gz' % fn
					

				cnt=0
				with gzip.open(fn, 'wb') as fh:
					fh.seek(0)
					rs=cur.fetchall()
					
					fh.write( '{"rowset": %s }' % json.dumps(rows_to_dict_list(rs,cur), default=datetime_handler, indent=4) )

			else:	
				fn=opt.out_json_file
				cnt=0
				with open(fn, 'wb') as fh:
					fh.seek(0)
					rs=cur.fetchall()
					
					fh.write( '{"rowset": %s }' % json.dumps(rows_to_dict_list(rs,cur), default=datetime_handler, indent=4) )

						
			
			
			cur.close()
			con.close()	
			exit_status	=SUCCESS	
		except cx_Oracle.DatabaseError as e:
			error, = e.args
			print ('#'*80)
			print ('#'*80)
			print(error.code)
			print(error.message)
			print(error.context)		
			print ('#'*80)
			print ('#'*80)	
			print(formatExceptionInfo())	
			
			raise
