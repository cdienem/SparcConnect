#!/bin/python


import os, time

from datetime import datetime
from pymongo import MongoClient
import sys
import subprocess

import shlex



class SparcConnect:
	""" Libary to communicate with cryoSPARC2 """
	def __init__(self, host, port, email):
	#Initiates the connection to the cryoSPARC database
	# TODO: check for the presence of cryosparcm cli!
	# Add ssh user
		self.host = host
		self.db = self.connectMongo(host, port)
		self.user = self.get_user_id(email)
		if self.user == None:
			print("Could not find a user with the specified email adress.")
			print("Terminating.")
			sys.exit()

	def get_user_id(self, username):
	# Returns the cryptic user ID of a given Email adress
		cs_users = self.db["users"]
		for user in cs_users.find( {} ):
			for email in user["emails"]:
				if email["address"] == username:
					return user["_id"]
					break
		return None

	def connectMongo(self, host, port):
	# Returns a Mongo DB pointer
		client = MongoClient(host,port)
		db = client.meteor
		return db

	def list_projects(self):
	# Returns a list of item with [id, title]
		projects = self.db["projects"]
		project_list = []
		for project in projects.find({'owner_user_id':self.user}):
			project_list.append([ project["uid"], project["title"]])
		return project_list

	def list_workspaces(self, pid):
	# Returns a list of items with [id, title]
		workspaces = self.db["workspaces"]
		workspace_list = []
		for workspace in workspaces.find({"project_uid":pid}):
			workspace_list.append([ workspace["uid"], workspace["title"]])
		return workspace_list

	def has_import_job(self, pid, wid):
	# Returns the job ID of the first import job found
	# Otherwise returns False
		jobs = self.db["jobs"]
		for job in jobs.find({"project_uid":pid, "type": "import_particles", "deleted": False, "status":"completed"}):
			if wid in job["workspace_uids"] and job["params_spec"] != {}:
				return job["uid"]
				break
		return False

	def has_abinitio_job(self, pid, wid):
	# Returns the job ID of the first ab-init job found
	# Otherwise returns False
		jobs = self.db["jobs"]
		for job in jobs.find({"project_uid":pid, "type": "homo_abinit", "deleted": False, "status": "completed"}):
			if wid in job["workspace_uids"]:
				return job["uid"]
				break
		return False

	def has_import_vol_job(self, pid, wid):
	# Returns the job ID of the first volume import job found
	# Otherwise returns False
		jobs = self.db["jobs"]
		for job in jobs.find({"project_uid":pid, "type": "import_volumes", "deleted": False, "status": "completed"}):
			if wid in job["workspace_uids"]:
				return job["uid"]
				break
		return False

	def get_jobs(self, pid, wid, jtype, status, sort="created_at", sort_dir=-1):
	# Returns a list of job IDs that match the given parameters
	# jtype and status can be given as json expressions
	# Sorts DESC on created_at by default
		jobs = self.db["jobs"]
		job_list = []
		for job in jobs.find({"project_uid":pid, "type": jtype, "deleted": False, "status": status}).sort(sort,sort_dir):
			if wid in job["workspace_uids"]:
				job_list.append(job["uid"])
		return job_list

	def get_abinit_K(self, pid, wid, jid):
	# Returns the number of ab initio classes of the specified jobs
	# TODO: Check that jobs is actually an ab initio
		jobs = self.db["jobs"]
		job = jobs.find_one({"project_uid":pid, "deleted": False, "uid": jid})
		if wid in job["workspace_uids"]:
			return int(job["params_base"]["abinit_K"]["value"])
		else:
			return False

	def get_parents(self, pid, wid, jid):
	# Returns a list of parent job IDs
		jobs = self.db["jobs"]
		job = jobs.find_one({"project_uid":pid, "deleted": False, "uid": jid})
		parents = []
		if wid in job["workspace_uids"]:
			for parent in job["parents"]:
				if not self.is_deleted(pid, wid, parent):
					parents.append(parent)
		return parents

	def get_children(self, pid, wid, jid):
	# Returns a list of child job IDs
		jobs = self.db["jobs"]
		job = jobs.find_one({"project_uid":pid, "deleted": False, "uid": jid})
		children = []
		if wid in job["workspace_uids"]:
			for child in job["children"]:
				if not self.is_deleted(pid, wid, child) and not self.is_failed(pid, wid, child):
					children.append(child)
		return children

	def get_import_path(self, pid, wid, jid):
	# Returns the meta data path of an import job
	# TODO: Check that the job actually is an import job
		jobs = self.db["jobs"]
		job = jobs.find_one({"project_uid":pid, "deleted": False, "uid": jid})
		if wid in job["workspace_uids"]:
			return job["params_spec"]["particle_meta_path"]["value"]
		else:
			return False 

	def get_file_particle_num(self, path):
	#Returns the number of particles in a particle star file
	# TODO: Check for file existence
		result = subprocess.Popen("more "+path+"| grep mrcs | wc -l", shell=True, stdout=subprocess.PIPE)
		return int(result.stdout.read().decode("UTF-8"))

	def get_import_particle_num(self, pid, wid, jid):
	#Returns the number of importet particles from an import job
	#TODO: Check that the job is actually an import job
		jobs = self.db["jobs"]
		job = jobs.find_one({"project_uid":pid, "deleted": False, "uid": jid})
		if wid in job["workspace_uids"]:
			return int(job["output_result_groups"][0]["num_items"])
		else:
			return False 

	def get_3d_particle_num(self, pid, wid, jid):
	# Returns the number of input particles that went inti a 3D refine job
		jobs = self.db["jobs"]
		job = jobs.find_one({"project_uid":pid, "deleted": False, "uid": jid})
		if wid in job["workspace_uids"]:
			return int(job["output_result_groups"][0]["num_items"])
		else:
			return False

	def get_selected_particle_num(self, pid, wid, jid):
	#Returns the number of selected particles from a 2D select job
	#TODO: Check that the job is actually a 2D select job
		jobs = self.db["jobs"]
		job = jobs.find_one({"project_uid":pid, "deleted": False, "uid": jid})
		if wid in job["workspace_uids"]:
			for output in job["output_result_groups"]:
				if output["title"] == "Particles selected":
					return int(output["num_items"])
					break
		else:
			return False

	def get_result_raw(self, pid, result):
	#Returns the absolute download path of a result
		command = shlex.quote("cryosparcm cli 'get_result_download_abs_path(\""+str(pid)+"\", \""+str(result)+"\")'")
		return self.exec_cli(command).strip()

	def select_2d_classes(self, pid, wid, parent_jid, classes):
	# Selects the specified classes from a 2D classification job
	# Returns the job ID od the 2D select job
	#TODO: Jobtype and data format sanity checks
		# Create a new 2d selection job
		cmd = shlex.quote("cryosparcm cli 'make_job(\"select_2D\", \""+str(pid)+"\", \""+str(wid)+"\", \""+str(self.user)+"\")'")
		new_select_jid = self.exec_cli(cmd).strip()
		
		cmd = shlex.quote("cryosparcm cli 'job_connect_group(\""+str(pid)+"\",\""+parent_jid+".particles\",\""+new_select_jid+".particles\")'")
		self.exec_cli(cmd)
		
		cmd = shlex.quote("cryosparcm cli 'job_connect_group(\""+str(pid)+"\",\""+parent_jid+".class_averages\",\""+new_select_jid+".templates\")'")
		self.exec_cli(cmd)

		# Update the selected template parameter
		jobs = self.db["jobs"]
		# Find the new select job
		job = jobs.find_one( {"uid": new_select_jid, "project_uid":pid} )
		# Modify it parameters
		job["params_base"]["selected_templates"]["value"] = ",".join(str(c) for c in classes)
		# Update its data base entry
		jobs.find_one_and_update( {"uid": new_select_jid, "project_uid":pid}, {'$set': job} )

		# Enqueue the new select job
		cmd = shlex.quote("cryosparcm cli 'enqueue_job(\""+str(pid)+"\", \""+str(new_select_jid)+"\", \"default\")'")
		self.exec_cli(cmd)
		return str(new_select_jid)

	def get_volumes(self, pid, wid, jid):
	#Returns a list of result names (JXX.volume_xy)
		jobs = self.db["jobs"]
		#Cast jid into a list
		if type(jid) != list:
			jid = [jid]
		
		volumes = []
		for input_job in jid:
			job = jobs.find_one({"project_uid":pid, "deleted": False, "uid": input_job})
			if wid in job["workspace_uids"]:
				for group in job["output_result_groups"]:
					if group["type"] == "volume":
						volumes.append(input_job+"."+group["name"])
				return volumes
			else:
				return False

	def import_particles(self, pid, wid, starfile):
		#do_import_particles_star(puid, wuid, uuid, abs_star_path
		# Expand cryosparcs $HOME
		if os.path.isfile(os.path.expanduser(starfile.replace("$HOME","~"))):
			command = shlex.quote("cryosparcm cli 'do_import_particles_star(\""+str(pid)+"\", \""+str(wid)+"\", \""+str(self.user)+"\", \""+str(starfile)+"\")'")
			return self.exec_cli(command).strip()
		else:
			print('Could not find specified import star file '+str(starfile))
			return False

	def start_2D_from_import(self, pid, wid, import_id, classes):
		#do_run_class_2D(puid, wuid, uuid, particle_group, num_classes=50):
		command = shlex.quote("cryosparcm cli 'do_run_class_2D(\""+str(pid)+"\", \""+str(wid)+"\", \""+str(self.user)+"\", \""+str(import_id)+".imported_particles\", "+str(classes)+")'")
		return self.exec_cli(command).strip()

	def start_abinit(self, pid, wid, particles, classes):
		#do_run_abinit(puid, wuid, uuid, particle_group, num_classes=1)
		command = shlex.quote("cryosparcm cli 'do_run_abinit(\""+str(pid)+"\", \""+str(wid)+"\", \""+str(self.user)+"\", \""+str(particles)+"\", "+str(classes)+")'")
		return self.exec_cli(command).strip()

	def start_3D_refinement(self, pid, wid, particles, references):
	# Uses the provides particles and volumes to start a 3D refienement
	# Number of references provided will define whether homo or hetero refine will be launched
	# Returns the job ID of the startet 3D refinement
		if len(references) > 0:
			if len(references) == 1:
				job_type = "homo_refine"
			else:
				job_type = "hetero_refine"

			#Recast particles into a list if just a string is given
			if type(particles) != list:
				particles = [particles]
 
			# Create the 3d refine job
			cmd = shlex.quote("cryosparcm cli 'make_job(\""+job_type+"\", \""+str(pid)+"\", \""+str(wid)+"\", \""+str(self.user)+"\")'")
			new_3d_jid = self.exec_cli(cmd).strip()
			
			# Connect particle sets to the new 3d job
			for particle_set in particles:
				cmd = shlex.quote("cryosparcm cli 'job_connect_group(\""+str(pid)+"\",\""+particle_set+"\",\""+new_3d_jid+".particles\")'")
				self.exec_cli(cmd)
			
			#Connect volumes as references
			for ref in references:
				cmd = shlex.quote("cryosparcm cli 'job_connect_group(\""+str(pid)+"\",\""+ref+"\",\""+new_3d_jid+".volume\")'")
				self.exec_cli(cmd)

			cmd = shlex.quote("cryosparcm cli 'enqueue_job(\""+str(pid)+"\", \""+str(new_3d_jid)+"\", \"default\")'")
			self.exec_cli(cmd)
			return str(new_3d_jid)


		else:
			return False

	def delete_failed_jobs(self, pid, wid):
	# Removes 
	# Can be weird to use as failed jobs might disappear quickly
		jobs = self.db["jobs"]
		failed_jobs = jobs.find({"project_uid":pid, "status": "failed"})
		for j in failed_jobs:
			if wid in j["workspace_uids"]:
				command = shlex.quote("cryosparcm cli 'delete_job(\""+str(pid)+"\", \""+str(j["uid"])+"\")'")
				return self.exec_cli(command).strip()

	def is_deleted(self, pid, wid, jid):
	# Returns True/False
		jobs = self.db["jobs"]
		job = jobs.find_one({"project_uid":pid, "uid": jid})
		if wid in job["workspace_uids"]:
			return bool(job["deleted"])

	def is_failed(self, pid, wid, jid):
	# Returns True/False
		jobs = self.db["jobs"]
		job = jobs.find_one({"project_uid":pid, "uid": jid})
		if wid in job["workspace_uids"]:
			if job["status"] == "failed":
				return True
			else:
				return False

	def exec_cli(self, command):
	# TODO: localhost execution
	# TODO: ssh username as parameter
		if self.host == "localhost":
			pass
		else:
			# THis is still hardcoded. Needs to be changed to the specified user
			cmd = "ssh sshuser@"+self.host+" "+str(command)
			output = subprocess.check_output(cmd, shell=True)
			return str(output.decode())
