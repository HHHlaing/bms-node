from collections import OrderedDict
from pyndn.util.common import Common

def config_split(s):
	result = []
	if s == "":
		return result

	whiteSpace = " \t\n\r"
	iStart = 0

	while True:
		while s[iStart] in whiteSpace:
			iStart += 1
			if iStart >= len(s):
				return result

		iEnd = iStart
		inQuotation = False
		token = ""
		while True:
			if inQuotation:
				if s[iEnd] == '\"':
					token += s[iStart:iEnd]
					iStart = iEnd + 1
					inQuotation = False
			else:
				if s[iEnd] == '\"':
					token += s[iStart:iEnd]
					iStart = iEnd + 1
					inQuotation = True
				else:
					if s[iEnd] in whiteSpace:
						break
			iEnd += 1
			if iEnd >= len(s):
				break

		token += s[iStart:iEnd]
		result.append(token)
		if iEnd >= len(s):
			return result

		iStart = iEnd



class BoostInfoTree(object):
	def __init__(self, value = None, parent = None):
		self.subtrees = OrderedDict()
		self.value = value
		self.parent = parent
		self.lastChild = None
	
	def addSubtree(self, treeName, newTree):
		if treeName in self.subtrees:
			raise ValueError('Configuration file has illegal names')
		else:
			self.subtrees[treeName] = newTree
		newTree.parent = self
		self.lastChild = newTree

	def createSubtree(self, treeName, value = None):
		newTree = BoostInfoTree(value, self)
		self.addSubtree(treeName, newTree)
		return newTree


class BoostInfoParser(object):
	def __init__(self):
		self._root = BoostInfoTree()

	def read(self, fileName):
		f = open(fileName, 'r')
		input = f.read()
		f.close()

		self._read(input, self._root)

	def _read(self, input, ctx):
		for line in input.splitlines():
			ctx = self._parseLine(line.strip(), ctx)
		return ctx

	def _parseLine(self, string, context):
		string = string.strip()
		if len(string) == 0:
			return context

		strings = config_split(string)
		isSectionStart = False
		isSectionEnd = False
		for s in strings:
			isSectionStart = isSectionStart or s == '{'
			isSectionEnd = isSectionEnd or s == '}'

		if not isSectionStart and not isSectionEnd:
			key = strings[0]
			if len(strings) > 1:
				val = strings[1]
			else:
				val = None
			newTree = context.createSubtree(key, val)
			return context

		sectionStart = string.find('{')
		if sectionStart > 0:
			firstPart = string[:sectionStart]
			secondPart = string[sectionStart:]
			ctx = self._parseLine(firstPart, context)
			return self._parseLine(secondPart, ctx)

		if string[0] == '{':
			if not context.lastChild == None:
				context = context.lastChild
			return context

		if string[0] == '}':
			context = context.parent
			return context

		raise RuntimeError('BoostInfoParser: input line is malformed.')

	def getRoot(self):
		"""
		:return: The root tree of this parser 
		:rtype: BoostInfoTree
		"""
		return self._root

	def getName(self, key = None):
		name = self._root.subtrees['node_prefix'].value
		if key == None:
			return name
		else:
			key = key.lstrip('/')
			
		if not self._root.subtrees['children'].subtrees.get(key) == None:
			name += key
			return name
		
		raise RuntimeError('BoostInfoParser: no this name component')

	def getDataType(self, dataType = None, key = None):
		if dataType == None:
			return False
		if key == None:
			if not self._root.subtrees['data'].subtrees.get(dataType) == None:
				return True
			else:
				return False
		else:
			if not self._root.subtrees['children'].subtrees['data'].subtrees.get(dataType) == None:
				return True
			else:
				return False

	def getProducerInterval(self, dataType, aggregationType = None):
		if aggregationType == None:
			if not self._root.subtrees['data'].subtrees[dataType].subtrees.get(aggregationType) == None:
				return self._root.subtrees['data'].subtrees[dataType].subtrees[aggregationType].value
			else:
				return False
		else:
			result = OrderedDict()
			for i in range(len(self._root.subtrees['data'].subtrees[dataType].subtrees)):
				string = self._root.subtrees['data'].subtrees[dataType].subtrees.items[i][0]
				result.append(string) =  self._root.subtrees['data'].subtrees[dataType].subtrees[string].value
			return result



