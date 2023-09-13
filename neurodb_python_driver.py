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

class ResultStatus (): #resultSet.status 的状态码对应错误描述
	ERROR_INFO=0 #错误消息
	PARSER_OK=1  #运行成功
	NO_MEM_ERR=2  #内存分配异常
	SYNTAX_ERR=3  #普通语法错误
	NO_Exp_ERR=4  #未找到此指令
	NO_LNK_ERR=5  #缺少关系
	NO_ARROW_ERR=6  #缺少箭头
	DOU_ARROW_ERR=7  #关系双箭头错误
	NO_HEAD_ERR=8  #缺少头节点
	NO_TAIL_ERR=9  #缺少尾结点
	CHAR_NUM_UL_ERR=10  #必须是字母数字下划线
	NOT_PATTERN_ERR=11  #不是模式表达式
	DUP_VAR_NM_ERR=12  #此变量已被使用
	NO_SM_TYPE_ERR=13  #数组中含有不相同的类型
	NO_SUP_TYPE=14  #不支持的数据类型
	WRON_EXP=15  #指令搭配错误
	NOT_SUPPORT=16  #暂不支持的指令
	WHERE_SYN_ERR=17  #where语句语法
	WHERE_RUN_ERR=18  #where运算语法
	NO_VAR_ERR=19  #未找到变量
	NO_PAIR_BRK=20  #缺失配对括号
	CLIST_HAS_LINK_ERR=21  #删除带有关边的节点
	CLIST_OPR_ERR=22  #数据操作错误
	ORDER_BY_SYN_ERR=23  #order by语句语法
	DEL_PATH_ERR=24  #不可删除路径
	UNDEFINED_VAR_ERR=25  #未定义的变量
	WHERE_PTN_NO_VAR_ERR=26  #where模式条件，独立连通图缺少变量
	NO_PROC_ERR=27  #不支持的存储过程
	CSV_FILE_ERR=28  #csv文件读取错误
	CSV_ROW_VAR_ERR=29   #csv变量属性名在列中未找到
	QUREY_TIMEOUT_ERR=30    #查询过载超时

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
	
	def close():
		self.client.close()
	
	def executeQuery(self,query):
		self.client.send(query.encode("utf-8"))
		resultSet = ResultSet()
		bts=self.client.recv(1)
		btsstr = str(bts, 'UTF-8')
		type=btsstr[0]
		if type == '@': #返回的是只有一个成功执行状态位的数据包
			resultSet.status=1
		elif type == '$': #返回的是包含错误消息的报错数据包
			resultSet.status = 0
			resultSet.msg= readLine(self.client)
		elif type == '#': #返回的是包含正常消息的消息数据包
			resultSet.status = 1
			resultSet.msg= readLine(self.client)
		elif type == '*': #返回的是图查询结果数据包
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
			if resultSet.results>0:
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
		return None


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
			l = self.deserializeCLink(cur, rd.types, rd.keyNames)
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
					l = self.getLinkById(rd.links, id)
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
result=driver.executeQuery("match (n)-[r]->(m) return n,r,m ")
print("ok")
