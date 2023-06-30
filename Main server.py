#!/usr/bin/python
import threading
from multiprocessing import Pool
import socket
import sys
import time
from queue import Queue
import gzip
import json
import shutil
from tkinter import messagebox
import tkinter
import tkinter.scrolledtext as st
import binascii
from itertools import count
from multiprocessing import Process
from tkinter import ttk

class Server:
	def __init__(self):
		global conn
		self.grandchildren=''
		self.direct_children='S:10.10.26.180'
		self.choices=[]
		self.tree=''
		self.host=''
		self.port=1111
		self.s=socket.socket()
		self.all_connections = []
		self.all_address = []
		self.start_time=None
		self.showlist=''
		self.CRC=''
		self.goal=''
		self.target=''
		self.filename='received.csv'
		self.options = [0,1,5,10]
		self.minute=5
		#threads
		socket_thread=threading.Thread(target=self.create_socket)
		acc=threading.Thread(target=self.accepting_connections)
		p1 = Process(target=self.check_CRC, name='Process_inc_forever')
		
		socket_thread.start()
		acc.start()
		
		#UI
		self.win=tkinter.Tk()
		self.win.title('THESIS')
		self.win.configure(bg="pink")
		
		self.chat_label=tkinter.Label(self.win,text='Flour Bin Data',bg='pink')
		self.chat_label.config(font=('Arial',12))
		self.chat_label.pack()
		
		self.avail=tkinter.Label(self.win, text='Available Nodes')
		self.avail.pack()
		
		self.viewlis=st.ScrolledText(self.win)
		self.viewlis.config(bg='white')
		self.viewlis.pack(padx=18,pady=5)
		
		self.input_label=tkinter.Label(self.win, text='Enter node to request (ex. N2):')
		self.input_label.config(bg='pink')
		self.input_label.pack()
		self.input_area=tkinter.Text(self.win,height=2)
		self.input_area.pack()
		
		self.request_button=tkinter.Button(self.win, text='Request Data', command=self.get_target)
		self.request_button.config(font=("Arial",12))
		self.request_button.pack(padx=20,pady=5)
		
		self.duration_label=tkinter.Label(self.win, text='Duration (in minutes):')
		self.duration_label.place(relx=0.2, rely=0.6,anchor='sw')
		self.duration_label.config(bg='pink')
		self.duration_label.pack()
		
		self.duration=ttk.Combobox(self.win)
		self.duration['values']= tuple(list(self.duration['values']) + self.options)
		self.duration.place(relx=0.1, rely=0.95)
		
		self.list_button=tkinter.Button(self.win, text='Generate List', command=self.list_connections)
		self.list_button.config(font=("Arial",12))
		self.list_button.place(relx=0.8, rely=0.8)
		self.list_button.pack(padx=10,pady=5)
		self.gui_done=True
		
		self.win.mainloop()
		
	def create_socket(self):
		print('socket created')
		try:
		
			self.s.bind((self.host, self.port))
			self.s.listen(5)
				
		except socket.error as msg:
			print('SOcket creation error:'+str(msg))
			
	def accepting_connections(self):
		print('accepting_connections')
		for c in self.all_connections:
			c.close()
		
		del self.all_connections[:]
		del self.all_address[:]
		while True:
			try:
				connect,address=self.s.accept()
				#self.s.blocking(1)
				self.all_connections.append(connect)
				self.all_address.append(address)
				self.list_connections()
			except:
				pass

	def list_connections(self):
		self.showlist=''
		self.grandchildren=''
		self.direct_children='S:10.10.26.180'
		self.choices=[]
		self.tree=''
		self.host=''
		for i,connect in enumerate(self.all_connections):
			self.grandchildren=self.receive_list(connect)
			self.tree=self.tree+' '+self.grandchildren
			self.direct_children=self.direct_children+' '+str(self.all_address[i][0])
			
		self.tree=self.direct_children+'  '+self.tree
		print(self.tree)
		self.tree.split('  ')
		#self.update_tree()
		
		print('\nMain routing path:'+str(self.tree)+'\n')
		self.tree=self.tree.split('  ')
		
		print('Available nodes:')
		self.viewlis.insert('end','Available nodes:\n')
		for x in self.tree:
			x=x.strip(' ')
			x=x[0:15]
			self.showlist=self.showlist+str(x)+'\n'
			print(str(x))
			self.choices.append(x)
		self.viewlis.insert('end',self.showlist)
	
	def branching(self):
		direct=''
		print('tree'+str(self.tree))
		#self.tree=self.tree.reverse()
		print('sssss'+str(self.tree))	
		if(self.direct_children.split(' ').count(str(self.goal))==1):
			print('here')
			for i, connect in enumerate(self.all_connections):
				if(str(self.goal)==self.all_address[i][0]):
					print('ddd'+str(direct))
					direct=self.all_address[i][0]
		else:
			for x in self.tree:
				print('x'+str(x))
				x=x.strip('')
				branch=x.split(' ')
				print('branch:'+str(branch))
				near=branch[1:15]
				print('near'+str(near)	)	
				if(branch.count(str(self.goal))):
					direct=str(''.join(near[:1])).split(':')[1]
				else:
					print('ok')
		print('direct:' +str(direct))
		self.send(direct)

	def update_tree(self):
		self.tree=self.tree.split('  ')
		new_list=[x for x in self.tree if x!='']
		self.tree=list(dict.fromkeys(new_list))
		
		for i, branch in enumerate(self.tree):
			branch=branch.split(' ')
			newt=[x for x in branch if x!='']
			newt=list(dict.fromkeys(newt))
			branch=' '.join(newt)
			self.tree[i]=branch
		self.tree=' '.join(self.tree)
		
		
	def receive_list(self,connect):
		connect.send(str.encode('list'))
		grandchildren=connect.recv(1024).decode('utf-8')
		return grandchildren
	
	def send(self,direct):
		global conn
		for i in range(len(self.all_address)):
			if(str(direct)==str(self.all_address[i][0])):
				conn=self.all_connections[i]
				continue
		str1='GOAL:'+str(self.goal)
		conn.send(str.encode(str1))
		self.receive(conn)
		
	def get_target(self):
		self.target=self.input_area.get(1.0,"end-1c")
		self.start_time=time.time()
		print('target:'+str(self.target))
		for x in self.choices:
			self.goal=x.split(':')
			
			if(str(self.goal[0])==self.target):
				print('goal:'+str(self.goal[1]))
				self.goal=self.goal[1]
				self.branching()
	def getdata(self):
		
		self.minute=self.duration.get()
		self.timer()
		
	def timer(self):
		print('stat')
		while True:
			self.list_connections()
			time.sleep(60*int(self.minute))	
	def receive(self,conn):
		data=conn.recv(1024)
		
		self.CRC=(str(data).split(':'))[1][:8]
		print(self.CRC)
		self.viewlis.insert('end',str('CRC: '+self.CRC))
		if (str(data)[2:5]=='CRC'):
			print('nisulod ra')
			try:
				#self.viewlis.insert('end','STATUS:RECEIVING FILE')
				with open(self.filename+'.gz','wb') as file:
					self.viewlis.insert('end','\nSTATUS:RECEIVING FILE')
					
					while True:
						file_data=conn.recv(4096*4096)
						if not file_data:
							end_time=time.time()
							hop=end_time-start_time
							print('req-send:'+str(hop))
							decompress()	
						if (str(file_data)[-6:]=='ethyl\''):
							file.write(file_data[:-6])
							break
						#if((t1-t0)>=10):
						#self.check_CRC()
						print('done')
						
						file.write(file_data)
					file.close()

				end_time=time.time()
				hop=end_time-self.start_time
				print('Request to receive:'+str(hop))
				self.viewlis.insert('end','\nRequest to receive:'+str(hop))
				self.check_CRC()
				#self.decompress()
			except:
				self.check_CRC()
				#	print('Error')
		else:
				print(data)
			
	def decompress(self):
		self.viewlis.insert('end','\nSTATUS:DECOMPRESSING')
		start_decompress=time.time()
		with gzip.open(self.filename+'.gz','rb') as f_in, open (self.filename,'wb') as f_out:
			shutil.copyfileobj(f_in,f_out)
		print('decompressed:'+self.filename)
		end_time=time.time()
		
		elapsed_time=start_decompress-end_time
		print('decompress:'+str(elapsed_time))
		self.viewlis.insert('end','\ndecompress:'+str(elapsed_time))

	def check_CRC(self):
		check =open(self.filename+'.gz','rb').read()
		check=(binascii.crc32(check) & 0xFFFFFFFF)
		crc_server="%08X"%check
		self.viewlis.insert('end','\nSERVER CRC:')
		self.viewlis.insert('end',str(crc_server))
		if crc_server==self.CRC:
			tkinter.messagebox.showinfo("File received", "File received, decompressed, and checked successfully")
			self.decompress()
		else: 
			msg=tkinter.messagebox.askyesno("CRC error", "Repeat process?")
			if msg=='yes':
				self.receive(self.goal)
			else:	
				pass
server=Server()


