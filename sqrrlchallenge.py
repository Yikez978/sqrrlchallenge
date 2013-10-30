# python integers are unbounded
# set a bound so that we can divide the key space
MAX_INT = 2**63 - 1

class Master:
	# assumes that num_tablets is a positive integer
	# assumes that server_names is a potentially empty list of server names
	# assumes that server names are non-empty strings
	def __init__(self, num_tablets, server_names):
		self.num_tablets = num_tablets
		self.server_names = server_names

		# self.assignments[i] is the server that owns tablet i
		if not server_names:
			# an empty string means that there is no server
			self.assignments = [""] * num_tablets
		else:
			# divide the tablets as equally as possible
			self.assignments = []
			for i in range(num_tablets):
				server = server_names[i * len(server_names) / num_tablets]
				self.assignments.append(server)

	# assumes that there is at least one server with tablets assigned to it
	def getServerLoads(self):
		loads = {}
		for s in self.server_names:
			loads[s] = 0

		for a in self.assignments:
			loads[a] += 1

		return loads

	# assumes loads maps server names to how many tablets are on that server
	# assumes that the server names in loads are non-empty strings
	@staticmethod
	def getMostLoadedServer(loads):
		most_loaded = None 
		for s in loads:
			if not most_loaded or loads[s] > max_load:
				most_loaded = s
				max_load = loads[s]

		return most_loaded, max_load

	# assumes loads maps server names to how many tablets are on that server
	# assumes that the server names in loads are non-empty strings
	@staticmethod
	def getLeastLoadedServer(loads):
		least_loaded = None 
		for s in loads:
			if not least_loaded or loads[s] < min_load:
				least_loaded = s
				min_load = loads[s]

		return least_loaded, min_load

    # assumes key is in [0, MAX_INT]
    # assumes that there is at least one tablet server
	def getServerForKey(self, key):
		# evenly divide the key space between all the tablets
		tablet_num = key * self.num_tablets / (MAX_INT + 1)
		return self.assignments[tablet_num]

	# assumes server_name is not in self.server_names
	def addServer(self, server_name):
		self.server_names.append(server_name)

		if len(self.server_names) == 1:
			# give all tablets to server_name
			self.assignments = [server_name] * self.num_tablets
		else:
			loads = self.getServerLoads()

			most_loaded, max_load = Master.getMostLoadedServer(loads)

			# transfer load from most loaded servers to server_name
			while max_load > loads[server_name] + 1:
				most_loaded_index = self.assignments.index(most_loaded)
				self.assignments[most_loaded_index] = server_name

				loads[server_name] += 1
				loads[most_loaded] -= 1

				most_loaded, max_load = Master.getMostLoadedServer(loads)

	# assumes server_name is in self.server_names
	def removeServer(self, server_name):
		if len(self.server_names) == 1:
			# there are no more servers left
			self.assignments = [""] * self.num_tablets
		else:
			loads = self.getServerLoads()

			# don't consider server_name when finding least loaded server
			server_name_load = loads[server_name]
			del loads[server_name]

			# transfer all of server_name's load to the least loaded servers
			for i in range(server_name_load):
				least_loaded, min_load = Master.getLeastLoadedServer(loads)

				server_name_index = self.assignments.index(server_name)
				self.assignments[server_name_index] = least_loaded

				loads[least_loaded] += 1

		self.server_names.remove(server_name)
