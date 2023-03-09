NEURODB_RETURNDATA = 1
NEURODB_SELECTDB = 2
NEURODB_EOF = 3
NEURODB_NODES = 6
NEURODB_LINKS = 7
NEURODB_EXIST = 17
NEURODB_NIL = 18
NEURODB_RECORD = 19
NEURODB_RECORDS = 20

NDB_6BITLEN = 0
NDB_14BITLEN = 1
NDB_32BITLEN = 2
NDB_ENCVAL = 3
# NDB_LENERR =UINT_MAX

VO_STRING = 1
VO_NUM = 2
VO_STRING_ARRY = 3
VO_NUM_ARRY = 4
VO_NODE = 5
VO_LINK = 6
VO_PATH = 7
VO_VAR = 8
VO_VAR_PATTERN = 9

class Node(object):
	def __init__(self,id,labels,properties ):
		self.id = id
		self.labels = labels
		self.properties  = properties 

class Link(object):
	def __init__(self,id,startNodeId,endNodeId,type,properties):
		self.id = id
		self.startNodeId =startNodeId 
		self.endNodeId=endNodeId
		self.type = type
		self.properties  = properties 

class ColVal(object):

	type = 0
	val = None
	aryLen  = 0

	def getNum(self):
		return float(self.val)
	def getNumArray(self):
		return list(self.val)
	def getString(self):
		return str(self.val)
	def getStringArry(self):
		return list(self.val)
	def getNode(self):
		self.val.__class__ = Node
		return self.val
	def getLink(self):
		self.val.__class__ = Link
		return self.val
	def getPath(self):
		return list(self.val)

class RecordSet(object):

	labels = []
	types = []
	keyNames = []
	nodes = []
	links = []
	records = [[]]

class ResultSet(object):

	status = 0
	cursor = 0
	results = 0
	addNodes = 0
	addLinks = 0
	modifyNodes = 0
	modifyLinks = 0
	deleteNodes = 0
	deleteLinks = 0
	msg = None
	recordSet = None

class StringCur(object):
	
	cur = 0
	s = None
	
	def __init__(self,bts):
		self.s=bts
		
	def get(self,size):
		bts=self.s[self.cur:self.cur+size]
		str = bts.decode('utf-8')
		self.cur=self.cur+size
		return str
		
	def getType(self):
		type= str(self.s[self.cur])
		self.cur=self.cur+1
		return type

import socket

class NeuroDBDriver(object):
	client = None
	def __init__(self,ip,port):
		self.client = socket.socket()
		self.client.connect((ip, port))
	
	def clase():
		self.client.close()
	
	def executeQuery(self,query):
		self.client.send(query.encode("utf-8"))
		resultSet = ResultSet()
		bts=self.client.recv(1)
		btsstr = str(bts, 'UTF-8')
		type=btsstr[0]
		if type == '@':
			resultSet.status=1
		elif type == '$':
			resultSet.msg= readLine(self.client)
		elif type == '#':
			resultSet.msg= readLine(self.client)
		elif type == '*':
			line = readLine(self.client)
			head = line.split(',')
			resultSet.status=int(head[0])
			resultSet.cursor=int(head[1])
			resultSet.results=int(head[2])
			resultSet.addNodes=int(head[3])
			resultSet.addLinks=int(head[4])
			resultSet.modifyNodes=int(head[5])
			resultSet.modifyLinks=int(head[6])
			resultSet.deleteNodes=int(head[7])
			resultSet.deleteLinks=int(head[8])
			
			bodyLen = int(head[9])
			body = self.client.recv(bodyLen)
			readLine(self.client)
			recordSet = self.deserializeReturnData(body)
			resultSet.recordSet=recordSet
		else:
			raise Exception("reply type erro")
		return resultSet
	
	
	def deserializeType(self,cur):
		return int(cur.getType())
	
	def deserializeUint(self,cur):
		buf = [0,0,0]
		buf[0] = int.from_bytes(cur.get(1).encode('utf-8') ,'little')
		buf[1] = int.from_bytes(cur.get(1).encode('utf-8') ,'little')
		buf[2] = int.from_bytes(cur.get(1).encode('utf-8') ,'little')
		return (buf[0]&0x7f)<<14|(buf[1]&0x7f)<<7|buf[2]
	
	def deserializeString(self,cur):
		len = self.deserializeUint(cur)
		val = cur.get(len)
		return val
	
	def deserializeStringList(self,cur):
		listlen = self.deserializeUint(cur)
		l = []
		while listlen > 0:
			s = self.deserializeString(cur)
			l.append(s)
			listlen=listlen-1
		return l
	
	def deserializeLabels(self,cur,labeList):
		listlen = self.deserializeUint(cur)
		l = []
		while listlen > 0:
			i = self.deserializeUint(cur)
			l.append(labeList[i])
			listlen=listlen-1
		return l
	
	
	def deserializeKVList(self,cur,keyNames):
		listlen = self.deserializeUint(cur)
		properties = {}
		while listlen > 0:
			i = self.deserializeUint(cur)
			key = keyNames[i]
			type = self.deserializeUint(cur)
			aryLen = 0
			val = ColVal()
			val.type=type
			if type == VO_STRING:
				val.val=self.deserializeString(cur)
			elif type == VO_NUM:
				doubleStr = self.deserializeString(cur)
				val.val=float(doubleStr)
			elif type == VO_STRING_ARRY:
				aryLen = self.deserializeUint(cur)
				valAry = []
				for i in range(0,aryLen):
					valAry[i] = self.deserializeString(cur)
				val.val=valAry
			elif type == VO_NUM_ARRY:
				aryLen = self.deserializeUint(cur)
				valAry = []
				for i in range(0,aryLen):
					doubleStr = self.deserializeString(cur)
					valAry[i] = float(doubleStr)
				val.val=valAry
			else:
				raise Exception("Error Type")
			properties[key] = val
			listlen=listlen-1
		
		return properties


	def deserializeCNode(self,cur, labels, keyNames):
		id = self.deserializeUint(cur)
		nlabels = self.deserializeLabels(cur, labels)
		kvs = self.deserializeKVList(cur, keyNames)
		n = Node(id, nlabels, kvs)
		return n

	def deserializeCLink(self,cur, types, keyNames):
		id = self.deserializeUint(cur)
		hid = self.deserializeUint(cur)
		tid = self.deserializeUint(cur)
		ty = self.deserializeType(cur)
		if ty == NEURODB_EXIST:
			typeIndex = self.deserializeUint(cur)
			type = types[typeIndex]
		#elif ty == NEURODB_NIL:
			
		kvs = self.deserializeKVList(cur, keyNames)
		l = Link(id, hid, tid, type, kvs)
		return l


	def getNodeById(self,nodes,id):
		for node in nodes:
			if node.id == id:
				return node
		return None

	def getLinkById(self,links, id):
		for link in links:
			if link.id == id:
				return link
		return null


	def deserializeReturnData( self,body):
		cur =  StringCur(body)
		rd =  RecordSet()
		path = None
		#/*读取labels、types、keyNames列表*/
		if self.deserializeType(cur) != NEURODB_RETURNDATA:
			raise Exception("Error Type")
		rd.labels=self.deserializeStringList(cur)
		rd.types=self.deserializeStringList(cur)
		rd.keyNames=self.deserializeStringList(cur)
		#/*读取节点列表*/
		if self.deserializeType(cur) != NEURODB_NODES:
			raise Exception("Error Type")
		cnt_nodes = self.deserializeUint(cur)
		for i in range(0,cnt_nodes):
			n = self.deserializeCNode(cur, rd.labels, rd.keyNames)
			rd.nodes.append(n)
		#/*读取关系列表*/
		if self.deserializeType(cur) != NEURODB_LINKS:
			raise Exception("Error Type")
		cnt_links = self.deserializeUint(cur)
		for i in range(0, cnt_links):
			l = self.deserializeCLink(cur, rd.getTypes(), rd.keyNames)
			rd.links.append(l)
		#/*读取return结果集列表*/
		if self.deserializeType(cur) != NEURODB_RECORDS:
			raise Exception("Error Type")
		cnt_records = self.deserializeUint(cur)
		for i in range(0,cnt_records):
			if self.deserializeType(cur) != NEURODB_RECORD:
				raise Exception("Error Type")
			cnt_column = self.deserializeUint(cur)
			record = []
			for i in range(0,cnt_column):
				aryLen = 0
				type = self.deserializeType(cur)
				val =  ColVal()
				val.type=type
				#if type == NEURODB_NIL:
				#/*val =NULL*/
				#} else 
				if type == VO_NODE:
					id = self.deserializeUint(cur)
					n = self.getNodeById(rd.nodes, id)
					val.val=n
				elif type == VO_LINK :
					id = self.deserializeUint(cur)
					l = self.getLinkById(rd.getLinks(), id)
					val.val=l
				elif type == VO_PATH:
					len = self.deserializeUint(cur)
					path = []
					for i in range(0,len):
						id = self.deserializeUint(cur)
						if i % 2 == 0:
							nd = self.getNodeById(rd.getNodes(), id)
							path.append(nd)
						else:
							lk = self.getLinkById(rd.getLinks(), id)
							path.append(lk)
					val.val=path
				elif type == VO_STRING:
					val.val=self.deserializeString(cur)
				elif type == VO_NUM:
					doubleStr = self.deserializeString(cur)
					val.val=float(doubleStr)
				elif type == VO_STRING_ARRY:
					aryLen = self.deserializeUint(cur)
					valAry = []
					for  i in range(0,aryLen):
						valAry[i] = self.deserializeString(cur)
					val.val=valAry
				elif type == VO_NUM_ARRY:
					aryLen = self.deserializeUint(cur)
					valAry =[]
					for  i in range(0,aryLen):
						doubleStr = self.deserializeString(cur)
						valAry[i] = float(doubleStr)
					val.val=valAry
				else:
					raise Exception("Error Type")
				record.append(val)
			rd.records.append(record)
		#/*读取结束标志*/
		if self.deserializeType(cur) != NEURODB_EOF:
			raise Exception("Error Type")
		return rd


def readLine(client):
	sb=""
	bts=client.recv(1)
	btsstr = str(bts, 'UTF-8')
	c=btsstr[0]
	while c!=None:
		sb=sb+c
		if c=='\n':
			break
		bts = client.recv(1)
		btsstr = str(bts, 'UTF-8')
		c = btsstr[0]
	return sb.replace("\r\n","")


driver = NeuroDBDriver("127.0.0.1",8839)
result=driver.executeQuery("match (n) return n")
print("ok")