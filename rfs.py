#!/usr/bin/python

from multiprocessing.pool import ThreadPool as Pool
import getpass, imaplib, ConfigParser
import email.message
import email.mime.multipart
import email.mime.application
import argparse
import os
import json
import threading
import time
import pwd
import grp
import base64
import hashlib
import sys
import logging

def hashfile(fn,start,stop):
    blocksize=65536
    hasher=hashlib.sha512()
    with open(fn, 'rb') as f:
        f.seek(start)
        remain=stop-start+1
        buf = f.read(min(remain,blocksize))
        while len(buf) > 0:
            hasher.update(buf)
            remain=remain-len(buf)
            buf = f.read(min(remain,blocksize))
    return hasher.hexdigest()

class finfo(dict):
    def __init__(self,fname=None,start=None,stop=None,json_str=None):
        if fname:
            #Attributes for the whole file
            self['fname']=fname
            os.stat_float_times(False)
            stats=os.stat(fname)
            self['atime']=stats.st_atime
            self['ctime']=stats.st_ctime
            self['mtime']=stats.st_mtime
            self['size']=stats.st_size
            self['user']=pwd.getpwuid(stats.st_uid).pw_name
            self['group']=grp.getgrgid(stats.st_gid).gr_name
            self['mode']=stats.st_mode
            #The following attributes are fragment level
            if start >= 0:
                self['sha512']=hashfile(fname,start,stop)
            self['start']=start
            self['stop']=stop

        elif json_str:
            f=json.loads(json_str)
            #Lift in known keys
            for k in ['fname','atime','ctime','mtime','size','user','group','mode', 'sha512','start','stop','frag_alloc']:
                if k in f:
                    self[k]=f[k]
                else:
                    self[k]=''
            #Bckwards compatibility stuff
            #if self['start']=='':
                #Not a fragment aware file 
             #   self['start']=0
              #  self['stop']=self.get_size()-1

    def to_json(self):
        return json.dumps(self,sort_keys=True,indent=4, separators=(',', ': '))

    def get_mode(self):
        dic = {'7':'rwx', '6' :'rw-', '5' : 'r-x', '4':'r--', '0': '---'}
        perm = str(oct(self['mode'])[-3:])
        return ''.join(dic.get(x,x) for x in perm)

    def get_user(self):
        return self['user']

    def get_group(self):
        return self['group']

    def get_size(self):
        return int(self['size'])

    def get_atime(self):
        return time.strftime('%x %X',time.localtime(self['atime']))
        
    def get_ctime(self):
        return time.strftime('%x %X',time.localtime(self['ctime']))

    def get_mtime(self):
        return time.strftime('%x %X',time.localtime(self['mtime']))

    def get_fname(self):
        return self['fname']

    def get_sha512(self):
        return self['sha512']

    def get_start(self):
        return int(self['start'])

    def get_stop(self):
        return int(self['stop'])

    def is_fragment(self):
        return True
        return (self.get_stop()-self.get_start()+1< self.get_size())

    def set_fragment_allocations(self,a):
        self['frag_alloc']=a


def load_config():
    config = ConfigParser.ConfigParser()
    config.read([ os.path.expanduser('~/.rfs.conf'), 'rfs.conf'])
    if len(config.sections())==0:
        logging.critical('config has no sections.')
        sys.exit(1)
    return config

#class imap4(imaplib.IMAP4_SSL):
#    def __init__(self, host):
#        imaplib.IMAP4_SSL.__init__(self,host)
#        if args.debug:
#            logging.info('setting debug')
#            self.debug=6

class Store(dict):
    def __init__(self,conf,section):
        self.name=section
        self.max_tries=5
        self['host']=conf.get(section,'host')
        self['user']=conf.get(section,'user')
        self['password']=conf.get(section,'password')
        self['folder']=conf.get(section,'folder')
        self['fragment_size']=conf.get(section,'fragment_size')
        self.connected=False
        self.selected=False

    def connect(self):
        #self.connection = imap4(self['host'])
        self.connection = imaplib.IMAP4_SSL(self['host'])
        if args.debug:
            logging.info('setting imap debug')
            self.connection.debug=6
        self.connection.login(self['user'],self['password'])
        self.connected=True

    def disconnect(self):
        try:
            self.connection.close()
        except:
            pass

        try:
            self.connection.logout()
        except:
            pass
        self.connected=False
        self.selected=False

    def df(self):
        tries=self.max_tries
        while tries:
            if not self.connected:
                self.connect()
            try:
                q_str=self.connection.getquotaroot('INBOX')
            except imaplib.IMAP4_SSL.error as e:
                print 'QQQ in excption handler'
                template = "(store/df) An exception of type {0} occured. Arguments:\n{1!r}"
                message = template.format(type(e).__name__, e.args)
                print message
                #logging ('Append caught exception %s',e)
                tries -= 1
                self.connected=False
                print 'WWW sleeping in excption handler'
                time.sleep(1)
                next
            else:
                q_used=1024*int(q_str[1][1][0].split()[2])
                q_total=1024*int(q_str[1][1][0].split()[3].split(')')[0])
                q_avail=q_total-q_used
                q_percent=100*q_used//q_total
                return [ q_total, q_used, q_avail, q_percent]
        logging.critical('(store/df) Faild to reconnect to %s, aborting', self.name)
        sys/exit(1)
        
    def rm(self,uid):
        logging.info('Removing %s:%s',self.name,uid)
        res = self.uid('copy', uid, '[Gmail]/Trash')
        new_uid=self.connection.untagged_responses['COPYUID'][0].split()[2]

        self.uid('store',uid, '+FLAGS', '\\Deleted')
        self.uid('expunge',uid)
        self.select('[Gmail]/Trash')
        self.uid('store',new_uid, '+FLAGS', '\\Deleted')
        self.uid('expunge',new_uid)

    def append(self, message):
        tries=self.max_tries
        while tries:
            if not self.connected:
                self.connect()
            try:
                self.connection.append(self['folder'], 0, 0, message)
            except imaplib.IMAP4_SSL.error as e:
                print 'QQQ in excption handler'
                template = "An exception of type {0} occured. Arguments:\n{1!r}"
                message = template.format(type(e).__name__, e.args)
                print message
                #logging ('Append caught exception %s',e)
                tries -= 1
                self.connected=False
                print 'WWW sleeping in excption handler'
                time.sleep(1)
                next
            else:
                #Pick the last entry in the APPENDUID list
                ret = self.connection.untagged_responses['APPENDUID'][-1].split()[1]
                return ret
        logging.critical('Critically failed to append to %s, giving up', self.name)
        sys.exit(1)

    def select(self, folder='INBOX', readonly=False):
        if not self.connected:
            self.connect()
        typ, num = self.connection.select(folder, readonly)
        if typ != 'OK':
            logging.warning('Folder %s not found, creating it', folder)
            self.connection.create(folder)
            typ, num = self.connection.select(folder, readonly)
            if typ != 'OK':
                logging.critical('Failed to create folder. Exiting.')
                sys.exit(1)
        self.selected = True
        return typ, num

    def uid(self,command, *args):
        if not self.selected:
            self.select(self['folder'])
        return self.connection.uid(command,*args)

def uid2dict(store,uid):
    typ, data = store.uid('fetch', uid, '(BODY[1])')
    if data == [None]:
        logging.warning('Tried to fetch a dict from %s:%s, which had no content',store.name,uid)
        return {}
    as_json=base64.b64decode(data[0][1])
    as_dict=json.loads(as_json)
    assert type(as_dict) is dict, "uid2dict will not return a dict"
    return as_dict

def dict2msgid(d):
    msg=email.mime.application.MIMEApplication(json.dumps(d,sort_keys=True,indent=4, separators=(',', ': ')),_encoder=email.encoders.encode_base64)
    msg['Subject']='dict'

    msg_str=msg.as_string()
   #FIXME: Get better sizing
    toc_a=allocate(len(msg_str)*2)
    if len(toc_a)>1:
        #We got an allocation split over >1 message, abort.
        logging.critical('FATAL: Allocation of dict cannot straddle multiple messages')
        sys.exit(1)
    new_uid = toc_a[0][0].append(msg_str)
    return toc_a[0][0],new_uid

def my_stat(fname):
    s={}
    stats=os.stat(fname)
    os.stat_float_times(False)
    stats=os.stat(fname)
    s['atime']=stats.st_atime
    s['ctime']=stats.st_ctime
    s['mtime']=stats.st_mtime
    s['size']=stats.st_size
    s['user']=pwd.getpwuid(stats.st_uid).pw_name
    s['group']=grp.getgrgid(stats.st_gid).gr_name
    s['mode']=stats.st_mode
    assert type(s) is dict, "my_stat will not return a dict"
    return s

def mode2str(mode):
        dic = {'7':'rwx', '6' :'rw-', '5' : 'r-x', '4':'r--', '0': '---'}
        perm = str(oct(mode)[-3:])
        return ''.join(dic.get(x,x) for x in perm)

class Frag(dict):
#Representation of a file fragment
    def __init__(self):
        self['MAGIC']='FILE FRAG V1'
        self['statinfo']=[]
        self['fname']=None
        self['start']=None
        self['stop']=None
        self['sha512']=None
        self.store=None #Set to a value when the frag is backed by a store
        self.uid=None #Set to a value when the frag is backed by a uid
        self.loaded=False
        self.backing_dev=None

    def __str__(self):
        if self.store:
            t_str='%s' % self.store.name
        else:
            t_str='<undef>'
        if self.uid:
            t_str=t_str+':%s ' %self.uid
        else:
            t_str=t_str+':<undef> '
        t_str=t_str+'(frag, %s..%s) '%(self['start'], self['stop'])
        t_str=t_str+'\t%s' % mode2str(self['statinfo']['mode'])
        t_str=t_str+' %s' % self['statinfo']['user']
        t_str=t_str+' %s\t' % self['statinfo']['group']
        t_str=t_str+' %s\t' % self['statinfo']['size']
        t_str=t_str+'\t%s' % time.strftime('%x %X',time.localtime(self['statinfo']['mtime']))
        t_str=t_str+'\t%s' % self['fname']
        #t_str=t_str+'\t%s' % self['sha512']
        return t_str
        
    def save_to_store(self,store):
        assert self.loaded == True, 'Trying to save an unloaded fragment to store'

        msg = email.mime.multipart.MIMEMultipart()
        msg['Subject']='(Frag) %s (%s-%s)' %(self['fname'],self['start'],self['stop'])

        part1=email.mime.application.MIMEApplication(json.dumps(self,sort_keys=True,indent=4, separators=(',', ': ')),_encoder=email.encoders.encode_base64)
        msg.attach(part1)

        if self.backing_dev == 'local':
            with open(self['fname'], 'rb') as f:
                f.seek(self['start'])
                buf = f.read(self['stop']-self['start']+1)
                part2=email.mime.application.MIMEApplication(buf)
            msg.attach(part2)
        else:
            logging.critical('saving from unknown backing dev')
            sys.exit(1)

        #Do the network interaction.
        new_uid = store.append(msg.as_string())
        self.uid=new_uid
        self.store=store
        return self.uid

    def do_load(self):
        assert self.loaded == False, 'ERROR: Trying to load in a used fragment'
        assert self.attached == True, 'ERROR: Trying to load an unattached fragment'
        d=uid2dict(self.store,self.uid)
        if not 'MAGIC' in d: 
            logging.warning('Tried to load a fragment without MAGIC')
            return False
        if d['MAGIC'] == self['MAGIC']:
            #It's a fragment for sure
            self['statinfo']=d['statinfo']
            self['start']=d['start']
            self['stop']=d['stop']
            self['sha512']=d['sha512']
            self['fname']=d['fname']
            self.loaded=True
            self.backing_dev='store'
            return True
        else:
            #It's one of ours, but not a frag
            return False

    def attach_to_store(self,store,uid):
        assert self.loaded == False, 'ERROR: Trying to attach a loaded fragment'
        self.store=store
        self.uid=uid
        self.attached=True

    def save_to_file(self):
        if not self.loaded:
            self.do_load()
        if self.backing_dev == 'store':
            typ, data = self.store.uid('fetch', self.uid, '(RFC822)')
            msg=email.message_from_string(data[0][1])
            if not msg.is_multipart():
                logging.critical('Error, message %s:%s is not multipart', store,uid)
                sys.exit(1)
            #loop the payload to find the relevant parts
            for payload in msg.get_payload():
                try:
                    finf = finfo(json_str=payload.get_payload(decode=True))
                except ValueError:
                    #Save the ref
                    pl=payload
                    #Not valid json, continue the loop
                    continue
            #FIXME update the metadata as well
            assert self['fname'] != '', 'fname cannot be empty string when saving to file'
            try: 
                f = open(self['fname'], 'r+b')
            except IOError:
                f = open(self['fname'],'wb')
            f.seek(self['start'])
            f.write(pl.get_payload(decode=True))
            f.close()

            fhash=hashfile(self['fname'], self['start'], self['stop'])
            if fhash != self['sha512']:
                logging.critical('ERROR: Retrieved file does not match expected sha512')
                sys.exit(1)
        else:
            logging.critical('cannot save_to_file() using backing_dev %s', self.backing_dev)
            sys.exit(1)


    def add_from_file(self,fname,start,stop):
        assert self.loaded == False, 'ERROR: Trying to add to a used fragment'
        self['statinfo']=my_stat(fname)
        self['start']=start
        self['stop']=stop
        self['sha512']=hashfile(fname,start,stop)
        self['fname']=fname
        self.loaded=True
        self.backing_dev='local'
        return True

def allocate(size):
    #return a list of allocations for a file
    # [ [ store, startpos in fname, stoppos in fname ], ...]
    #While honoring:
    #   per-store fragment_size
    #and  optimizing for:
    #   TBD

    avail={}
    #Get the current free space on the stores
    for s in my_stores:
        q_tot, q_used, q_avail, q_used = s.df()
        avail[s.name] = q_avail
       
    remain=size
    pos=0
    allocations=[]
    while remain>0:
        #iterate over the stores
        for s in my_stores:
            #print 'avail: %s , frag size %s, remain %s' % (avail[s.name],1024*int(s['fragment_size']),remain)
            chunk=min(avail[s.name],1024*int(s['fragment_size']),remain)
            #so, we can allocate chunk kbytes and it is at most what we need
            allocation=[]
            allocation = [ s, pos, pos+chunk-1]
            #print pos
            #print chunk
            allocations.append(allocation)
            #Update file offsets
            remain -= chunk
            pos += chunk
            #Update avail space
            #print 'avail %s, chunk %s' %(avail[s.name],chunk)
            avail[s.name] -= chunk
            if remain==0:
                return allocations
def uploader(fname, chunks,resp):
     for chunk in chunks:
        frag=Frag()
        frag.add_from_file(fname,chunk[1],chunk[2])
        new_u = frag.save_to_store(chunk[0])
        resp.append([[chunk[0],new_u],chunk[1],chunk[2]])
 
class Fidx(dict):
#Index for a fragmented file
    def __init__(self):
        self['MAGIC']='FILE IDX V1'
        self['statinfo']=[]
        self['fragments']=[]
        self['sha512']=None
        self['fname']=None
        self.frags=[]
        self.store=None
        self.uid=None

    def __str__(self):
        return self.ls(Long=True,imap=True)
 
    def ls(self,Long=False,imap=False):
        t_str=''
        if imap:
            if self.store:
                t_str='%s' % self.store.name
            else:
                t_str='<undef>'
            if self.uid:
                t_str=t_str+':%s ' %self.uid
            else:
                t_str=t_str+':<undef> '
            if len(self['fragments'])>0:
                t_str=t_str+'(fidx, '
                for f in self['fragments']:
                    t_str=t_str+'%s:%s ' %(f[0][0],f[0][1])
                t_str=t_str+')\t'
        if Long:
            t_str=t_str+'%s' % mode2str(self['statinfo']['mode'])
            t_str=t_str+' %s' % self['statinfo']['user']
            t_str=t_str+' %s\t' % self['statinfo']['group']
            t_str=t_str+' %s\t' % self['statinfo']['size']
            t_str=t_str+'\t%s\t' % time.strftime('%x %X',time.localtime(self['statinfo']['mtime']))
        t_str=t_str+'%s' % self['fname']
                #t_str=t_str+'\t%s' % self['sha512']

        return t_str
        
    #uid is the uid of the index message
    def load_from_store(self, store, uid):
        d = uid2dict(store,uid)
        if not 'MAGIC' in d: 
            logging.warning('Tried to load a fidx without MAGIC')
            return False
        if d['MAGIC'] == self['MAGIC']:
            #It's a file index for sure
            self['statinfo']=d['statinfo']
            self['sha512']=d['sha512']
            self['fname']=d['fname']
            self.loaded=True
            self.store=store
            self.uid=uid
            self.backing_dev='store'
            self['fragments']=d['fragments']
            for backing, start, stop  in d['fragments']:
                s=store_name2store(backing[0])
                frag=Frag()
                frag.attach_to_store(s,backing[1])
                self.frags.append(frag)
            return True
        else:
            #It's one of ours, but not a fidx
            return False

    def save_to_store(self):
        return dict2msgid(self)

    def save_to_file(self):
        for frag in self['fragments']:
            frag.save_to_file()
    
    def add_fragment(self, alloc):
        self.frags.append(alloc)
        self['fragments'].append([[alloc[0][0].name,alloc[0][1]],alloc[1],alloc[2]])

    def add_statinfo(self, fname):
        #Attributes for the whole file
        self['statinfo']=my_stat(fname)
        self['sha512']=hashfile(fname,0,self['statinfo']['size']-1)
        self['fname']=fname

    def add_file(self,fname):
        a=allocate(os.stat(fname).st_size)
        #for chunk in a:
        #    frag=Frag()
        #    frag.add_from_file(fname,chunk[1],chunk[2])
        #    new_u = frag.save_to_store(chunk[0])
        #    self.add_fragment([[chunk[0],new_u],chunk[1],chunk[2]])
        #self.add_statinfo(fname)
        threads={}
        res={}
        for s in my_stores:
             chunks = [ chunk for chunk in a if chunk[0].name == s.name ]
             res[s.name]=[]
             t = threading.Thread(target=uploader, args = (fname, chunks,res[s.name]))
             t.daemon = True
             threads[s.name]=t
             threads[s.name].start()

        #Wait for them to finish
        for s in my_stores:
            logging.info('waiting for %s', s.name)
            threads[s.name].join()
            for chunk in res[s.name]:
                self.add_fragment(chunk)
 
class Toc(dict):
    #A toc as an unsorted array of store:uid tuples, not including fragements (only their heads)
    def __init__(self):
        self['toc']=[]
        self['MAGIC']='RFS TOC V1'
        self['rev']=0
        self.sources=[]

    def __str__(self):
        if self.store:
            t_str='%s' % self.store.name
        else:
            t_str='<undef>'
        if self.uid:
            t_str=t_str+':%s ' %self.uid
        else:
            t_str=t_str+':<undef> '
        t_str=t_str+'(toc) '
        return t_str

    def load_from_store(self, store, uid):
        d = uid2dict(store,uid)
        if not 'MAGIC' in d: 
            logging.warning('Tried to load a toc without MAGIC')
            return False
        if d['MAGIC'] == self['MAGIC']:
            #It's a toc for sure
            self.loaded=True
            self.store=store
            self.uid=uid
            self.backing_dev='store'
            return True
        else:
            logging.warning('Tried to load a non-toc (%s) as toc', d['MAGIC'])
            return False


    def load_from_stores(self):
    #Populate toc from a store hosted toc
    #Load from all stores, secuing that all finds are equal)
        for s in my_stores:
            #First path: Try to find a flagged and proper message
            ret, uids = s.uid('search', None, 'FLAGGED')
            highest_rev = 0
            wanted_uid = 0
            d={}
            for idx, uid in enumerate(uids[0].split()):
                self.sources.append([s.name, uid])
                if uid=='':
                    next
                d[idx]=uid2dict(s, uid)
                if d[idx]['rev'] > highest_rev:
                    highest_rev = d[idx]['rev']
                    wanted_idx = idx
            #We have a candidate from a store, check it vs. already known stores

            if highest_rev > 0 and self['rev'] < d[wanted_idx]['rev']:
                self.update(d[wanted_idx])
        if self['rev'] > 0:
            #We have found a stored toc, Be happy
            return
        logging.warning('No message flagged as toc found')

    def add_msgid(self,record):
        self['toc'].append(record)

    def save_to_stores(self):
        #Save the toc to all stores
        #bump the rev
        self['rev'] = self['rev'] + 1
        part=email.mime.application.MIMEApplication(json.dumps(self,sort_keys=True,indent=4, separators=(',', ': ')),_encoder=email.encoders.encode_base64)
        part['Subject']= 'rfs toc v1'

        for s in my_stores:
            new_uid = s.append(part.as_string())
            print 'new_uid %s' %new_uid
            s.uid('STORE', new_uid, '+FLAGS', '\FLAGGED')
            ret, uids = s.uid('search', None, 'FLAGGED')
            #secure only latest toc is flagged
            print uids
            for uid in uids[0].split():
                print 'testing uid %s' %uid
                if uid != new_uid:
                    s.rm(uid)

    def add_fidx(self,fname):
        fidx=Fidx()
        fidx.add_file(args.fname)
        fidx.add_statinfo(args.fname)
        new_s, new_u = fidx.save_to_store()

        print 'saving rfs toc'
        #Now, after the file toc, update the rfs toc
        self.load_from_stores()
        self.add_msgid([new_s.name,new_u])

       

#def fetch_using_rfc822(M,uid):
#    typ, data = M.uid('fetch', uid, '(RFC822)')
#    msg=email.message_from_string(data[0][1])
#    for payload in msg.get_payload():
#        try:
#            finf = finfo(json_str=payload.get_payload(decode=True))
#            break
#        except ValueError:
#            continue
#    return finf

#def fetch_using_body_one(store,uid):
#    typ, data = store.uid('fetch', uid, '(BODY[1])')
#    #print data
#    s=base64.b64decode(data[0][1])
#    finf = finfo(json_str=s)
#    return finf

def list_msgid(store,uid,Long=False,imap=False):
    #Test for frag
    frag=Frag()
    frag.attach_to_store(store,uid)
    if frag.do_load():
        print frag
        return
    #test for fidx
    fidx=Fidx()
    if fidx.load_from_store(store,uid):
        print fidx.ls(Long=Long,imap=imap)
        return
    #test for toc
    toc=Toc()
    if toc.load_from_store(store,uid):
        print toc
        return
    print '%s:%s, has unknown content' %(store.name,uid)
    return
 
def store_name2store(store_name):
    for s in my_stores:
        if s.name == store_name:
            return s

def cmd_list(args):
    wanted_uids=[]
    wanted_store=None
    if args.msgid:
        if ':' in args.msgid:
            wanted_store=args.msgid.split(':')[0]
            wanted_uids.append(args.msgid.split(':')[1])
        else:
            logging.critical('msgid has to have the format <store>:<uid>')
            sys.exit(1)

    if not args.notoc:
        #use the toc
        t = Toc()
        t.load_from_stores()
        for s, uid in t['toc']:
            if wanted_store!= None and wanted_store!=s:
                next

            list_msgid(store_name2store(s), uid,args.format_long,args.imap)
        return

    for store in my_stores:
        if wanted_store!= None and wanted_store!=store.name:
            next
        if len(wanted_uids)==0:
            typ, wanted_uids = store.uid('search' ,None, 'ALL')
            if wanted_uids[0]=='':
                next
        for uid in wanted_uids[0].split():
            list_msgid(store,uid,args.format_long)
        wanted_uids=[]

def cmd_put(args):
    toc=Toc()
    toc.add_fidx(args.fname)
    toc.save_to_stores()
    return
    x#fidx=Fidx()
    #a=allocate(os.stat(args.fname).st_size)
    #for chunk in a:
    #    frag=Frag()
    #    frag.add_from_file(args.fname,chunk[1],chunk[2])
    #    new_u = frag.save_to_store(chunk[0])
    #    fidx.add_fragment([[chunk[0],new_u],chunk[1],chunk[2]])
    #fidx.add_file(args.fname)
    #Ok, now we have uploaded all fragments, make a final new message with a file ToC.
    #fidx.add_statinfo(args.fname)

    #FIXME: assume it fits in 100KB
    toc_a=allocate(100*1024)
    if len(toc_a)>1:
        #We got an allocation split over >1 store, abort.
        print 'FATAL: Allocation of TOC cannot straddle multiple stores'
        sys.exit(1)

    new_u = fidx.save_to_store(toc_a[0][0])

    print 'saving rfs toc'
    #Now, after the file toc, update the rfs toc
    rfs_toc=Toc()
    rfs_toc.load_from_stores()
    rfs_toc.add_msgid([toc_a[0][0].name,new_u])
    rfs_toc.save_to_stores()

def cmd_get(args):

    if not ':' in args.msgid:
        print 'msgid has to have <store>:<uid> format'
        sys.exit(1)

    store_name, uid = args.msgid.split(':')
    store = store_name2store(store_name)

    #Test for frag
    frag=Frag()
    frag.attach_to_store(store,uid)
    if frag.do_load():
        frag.save_to_file()
        return
    #test for fidx
    fidx=Fidx()
    if fidx.load_from_store(store,uid):
        print fidx
        return
    print 'store %s, uid %s, has unknown content' %(store.name,uid)
    return
    typ, data = s.uid('fetch', uid, '(RFC822)')
    msg=email.message_from_string(data[0][1])
    if not msg.is_multipart():
        print 'Error, message uid %s in store %s is not multipart' %(uid,store)
        quit()
    #loop the payload to find the relevant parts
    for payload in msg.get_payload():
        try:
            finf = finfo(json_str=payload.get_payload(decode=True))
        except ValueError:
            #Save the ref
            pl=payload
            #Not valid json, continue the loop
            continue
    #FIXME update the metadata as well
    if finf.get_fname()=='':
        fname='rfs_temp_name'
        print 'WARNING: No filename found, using rts_temp_name'
    else:
        fname=finf.get_fname()
    f = open(fname,'wb')
    f.write(pl.get_payload(decode=True))
    f.close()

    fhash=hashfile(fname,0,os.stat(fname).st_size-1)
    if fhash != finf.get_sha512():
        print 'ERROR: Retrieved file does not match expected sha512'

def cmd_df(args):
    config = load_config()

    print 'Remote\t\t\t\t1K-blocks\tUsed\tAvailable\tUse\tStore'
    for store in config.get('rfs','stores').split():
        s=Store(config,store)
        s.connect()
        tot, used, free, percent = s.df()
        print '%s@%s/%s\t%s\t%s\t%s\t%s%%\t%s' % (s['user'],s['host'],s['folder'],tot//1024,used//1024,free//1024,percent,store)


def cmd_rm(args):
    store, uid = args.msgid.split(':')
    for s in my_stores:
        if s.name == store:
            break
    s.rm(uid)

def cmd_dump(args):
    store, uid = args.msgid.split(':')
    for s in my_stores:
        if s.name == store:
            break


    if args.bodystructure:
        typ, data = s.uid('fetch', uid, '(BODYSTRUCTURE)')
        print data
        return
    typ, data = s.uid('fetch', uid, '(RFC822)')
    if args.rfc822:
        print data[0][1]
    elif args.decode:
        msg=email.message_from_string(data[0][1])
        if msg.is_multipart():
            for part in msg.walk():
                print '== Headers'
                for pair in part.items():
                    print pair[0]+': '+pair[1]
                print '-- body'
                print part.get_payload(decode=True)
        else:
            print msg.get_payload(decode=True)
 
def cmd_gen_toc(args):
    toc=Toc()
    for store in my_stores:
        typ, wanted_uids = store.uid('search' ,None, 'ALL')
        for uid in wanted_uids[0].split():
            #Fast path
            finf=fetch_using_body_one(store,uid)
            #Fallback
            #finf = fetch_using_rfc822(M,uid)
            if not finf.is_fragment():
                toc.add_msgid([store.name,uid])
    print toc
    toc.save_to_stores()



config = load_config()
my_confd_stores = config.get('rfs','stores').split()
my_stores=[]
for s in my_confd_stores:
    my_stores.append(Store(config, s))


logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

parser = argparse.ArgumentParser(description='RFS, remote file store.')
parser.add_argument('-d',action='store_true',help='-d debug',dest='debug')
subparsers = parser.add_subparsers()

parser_list = subparsers.add_parser('ls', help='ls help')
parser_list.add_argument('-l', action='store_true',help='long format',dest='format_long')
parser_list.add_argument('--msgid', help='List only the specified msgid')
parser_list.add_argument('--notoc', help='List not using the toc, use the backing messages',action='store_true',dest='notoc')
parser_list.add_argument('--imap', help='Include imap details in the listing',action='store_true',dest='imap')

parser_list.set_defaults(func=cmd_list)

parser_put = subparsers.add_parser('put', help='put help')
parser_put.add_argument('--store', help='The store to use, if none provided one is picked')
parser_put.add_argument('fname', help='filename to put')
parser_put.set_defaults(func=cmd_put)

parser_get = subparsers.add_parser('get', help='get help')
parser_get.add_argument('msgid', help='The msgid to get')
parser_get.set_defaults(func=cmd_get)

parser_df = subparsers.add_parser('df', help='df help')
parser_df.set_defaults(func=cmd_df)

parser_rm = subparsers.add_parser('rm', help='rm help')
parser_rm.add_argument('msgid', help='msgid to remove')
parser_rm.set_defaults(func=cmd_rm)

parser_dump = subparsers.add_parser('dump', help='dump help')
parser_dump.add_argument('--rfc822', action='store_true', help='dump raw rfc822 container',dest='rfc822')
parser_dump.add_argument('--decode', action='store_true', help='dump decoded rfc822 container',dest='decode')
parser_dump.add_argument('--bodystructure', action='store_true', help='dump IMAP BODYSTRUCUTRE f;r the rfc822 container ',dest='bodystructure')
parser_dump.add_argument('msgid', help='msgid to dump')
parser_dump.set_defaults(func=cmd_dump)

parser_gen_toc = subparsers.add_parser('gen_toc', help='rm help')
parser_gen_toc.set_defaults(func=cmd_gen_toc)


args = parser.parse_args()
args.func(args)
