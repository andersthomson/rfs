#!/usr/bin/python

import getpass, imaplib, ConfigParser
import email.message
import email.mime.multipart
import email.mime.application
import argparse
import os
import json
import time
import pwd
import grp
import base64
import hashlib
import sys

def hashfile(fn,start,stop):
    hasher=hashlib.sha512()
    with open(fn, 'rb') as f:
        f.seek(start)
        buf = f.read(stop-start+1)
        hasher.update(buf)
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
            #print json_str
            f=json.loads(json_str)
            #Lift in known keys
            for k in ['fname','atime','ctime','mtime','size','user','group','mode', 'sha512','start','stop','frag_alloc']:
                if k in f:
                    self[k]=f[k]
                else:
                    self[k]=''
            #Bckwards compatibility stuff
            if self['start']=='':
                #Not a fragment aware file 
                self['start']=0
                self['stop']=self.get_size()-1

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
        return (self.get_stop()-self.get_start()+1< self.get_size())

    def set_fragment_allocations(self,a):
        self['frag_alloc']=a


def load_config():
    config = ConfigParser.ConfigParser()
    config.read([ os.path.expanduser('~/.rfs.conf'), 'rfs.conf'])
    if len(config.sections())==0:
        print 'config has no sections.'
        sys.exit(1)
    return config

class imap4(imaplib.IMAP4_SSL):
    def __init__(self, host):
        imaplib.IMAP4_SSL.__init__(self,host)
        if args.debug:
            print 'setting debug'
            self.debug=5


    def do_select(self, mailbox):
        typ, num = imaplib.IMAP4_SSL.select(self, mailbox)
        if typ != 'OK':
            print 'Folder %s not found, creating it' % mailbox
            imaplib.IMAP4_SSL.create(select, mailbox)
            typ, num = imaplib.IMAP4_SSL.select(self, mailbox)
            if typ != 'OK':
                print 'Failed to create folder. Exiting.'
                sys.exit(1)
        return typ, num

    def do_append(self,mailbox, flags, date_time, message):
        imaplib.IMAP4_SSL.append(self, mailbox, flags, date_time, message)
        return self.untagged_responses['APPENDUID'][0].split()[1]

class store_c(dict):
    def __init__(self,conf,section):
        self['host']=conf.get(section,'host')
        self['user']=conf.get(section,'user')
        self['password']=conf.get(section,'password')
        self['folder']=conf.get(section,'mailbox')
        self['fragment_size']=conf.get(section,'fragment_size')
        self.connected=False

    def connect(self):
        self.connection = imap4(self['host'])
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

    def df(self):
        if not self.connected:
            self.connect()
        q_str=self.connection.getquotaroot('INBOX')
        q_used=1024*int(q_str[1][1][0].split()[2])
        q_total=1024*int(q_str[1][1][0].split()[3].split(')')[0])

        q_avail=q_total-q_used
        return [ q_total, q_used, q_avail ]
        
def imap_connect(host,user,password,mailbox):
    M = imap4(host)
    M.login(user,password)
    typ, num = M.do_select(mailbox)
    return M

def fetch_using_rfc822(M,uid):
    typ, data = M.uid('fetch', uid, '(RFC822)')
    msg=email.message_from_string(data[0][1])
    for payload in msg.get_payload():
        try:
            finf = finfo(json_str=payload.get_payload(decode=True))
            break
        except ValueError:
            continue
    return finf

def fetch_using_body_one(M,uid):
    typ, data = M.uid('fetch', uid, '(BODY[1])')
    s=base64.b64decode(data[0][1])
    finf = finfo(json_str=s)
    return finf

def cmd_list(args):
    config=load_config()

    storeset=[]
    wanted_uids=[]
    if args.msgid:
        if ':' in args.msgid:
            storeset.append(args.msgid.split(':')[0])
            wanted_uids.append(args.msgid.split(':')[1])
        else:
            print 'msgid has to have the format <store>:<uid>'
            sys.exit(1)
    else:
        storeset=config.get('rfs','stores').split()

    for store in storeset:
        M = imap_connect(config.get(store, 'host'),config.get(store, 'user'),config.get(store, 'password'),config.get(store, 'mailbox'))
        if len(wanted_uids)==0:
            typ, wanted_uids = M.uid('search' ,None, 'ALL')
            if wanted_uids[0]=='':
                continue
        for uid in wanted_uids[0].split():
            #Fast path
            finf=fetch_using_body_one(M,uid)
            #Fallback
            #finf = fetch_using_rfc822(M,uid)
            if args.format_long:
                t_str='%s' % store
                t_str=t_str+':%s' % uid
                t_str=t_str+'\t%s' % finf.get_mode()
                t_str=t_str+' %s' % finf.get_user()
                t_str=t_str+' %s\t' % finf.get_group()
                t_str=t_str+'{}'.format(finf.get_stop()-finf.get_start()+1)
                if finf.is_fragment():
                    t_str=t_str+'({}..{})'.format(finf.get_start(),finf.get_stop())
                #t_str=t_str+'\t%s' % finf.get_atime()
                #t_str=t_str+'%s' % finf.get_ctime()
                t_str=t_str+'\t%s' % finf.get_mtime()
                t_str=t_str+'\t%s' % finf.get_fname()
                print t_str
            else:
                print  '%s:%s %s' %(store,uid,finf.get_fname())
        wanted_uids=[]
        M.close()
        M.logout()

def put_fragment(config,store,fname, start, stop):
    msg = email.mime.multipart.MIMEMultipart()
    msg['Subject']='%s (%s-%s)' %(fname,start,stop)

    #Prepare metadata
    info=finfo(fname=args.fname,start=start,stop=stop)
    part=email.mime.application.MIMEApplication(info.to_json(),_encoder=email.encoders.encode_base64)
    msg.attach(part)

    with open(fname, 'rb') as f:
        f.seek(start)
        buf = f.read(stop-start+1)
        part=email.mime.application.MIMEApplication(buf)
    msg.attach(part)

    #Start network interactions.
    M = imap_connect(config.get(store, 'host'),config.get(store, 'user'),config.get(store, 'password'),config.get(store, 'mailbox'))
    new_uid=M.do_append(config.get(store, 'mailbox'), '', '', msg.as_string())
    M.close()
    M.logout()
    return new_uid

def allocate(config, size):
    #return a list of allocations for a file
    # [ [ store, startpos in fname, stoppos in fname ], ...]
    #While honoring:
    #   per-store fragment_size as set in config
    #and  optimizing for:
    #   TBD


    avail={}
    #Get the current free space on the stores
    for store in config.get('rfs','stores').split():

        s=store_c(config,store)
        q_tot, q_used, q_avail = s.df()
        s.disconnect()
        avail[store] = q_avail // 1024
       
    remain=size
    pos=0
    
    allocations=[]
    while remain>0:
        #iterate over the stores
        for s in config.get('rfs','stores').split():
            chunk=min(1024*avail[s],1024*int(config.get(s,'fragment_size')),remain)
            #so, we can allocate chunk kbytes and it is at most what we need
            allocation=[]
            allocation = [ s, pos, pos+chunk-1]
            allocations.append(allocation)
            #Update file offsets
            remain -= chunk
            pos += chunk
            #Update availability
            avail[s] -= chunk
            if remain==0:
                return allocations

def cmd_put(args):
    config = load_config()

    #Check if a store has been given
    if args.store:
       store=args.store
    else:
        #FIXME: This should be dynamic based on free space etc
        store = config.get('rfs','stores').split()[1]
    a=allocate(config, os.stat(args.fname).st_size)
    for chunk in a:
        new_uid=put_fragment(config,chunk[0],args.fname,chunk[1],chunk[2])
        chunk.append(new_uid)
    #Ok, now we have uploaded all fragments, make a final new message with a file ToC.
    info=finfo(fname=args.fname,start=-1,stop=-1)
    info.set_fragment_allocations(a)
    part=email.mime.application.MIMEApplication(info.to_json(),_encoder=email.encoders.encode_base64)

    msg = email.mime.multipart.MIMEMultipart()
    msg['Subject']='%s (toc)' %(args.fname)

    msg.attach(part)

    #FIXME: assume it fits in 100KB
    toc_a=allocate(config,100*1024)
    #print toc_a
    #print len(toc_a)
    if len(toc_a)>1:
        #We got an allocation split over >1 store, abort.
        print 'Allocation of TOC cannot straddle multiple stores'
        sys.exit(1)

    #M = imap_connect(config.get(toc_a[0][0], 'host'),config.get(toc_a[0][0], 'user'),config.get(toc_a[0][0], 'password'),config.get(toc_a[0][0], 'mailbox'))
    #new_uid=M.do_append(config.get(toc_a[0][0], 'mailbox'), '', '', msg.as_string())
    #M.close()
    #M.logout()

def cmd_get(args):
    config = load_config()

    if not ':' in args.msgid:
        print 'msgid has to have <store>:<uid> format'
        sys.exit(1)

    store, uid = args.msgid.split(':')

    M = imap_connect(config.get(store, 'host'),config.get(store, 'user'),config.get(store, 'password'),config.get(store, 'mailbox'))
    
    typ, data = M.uid('fetch', uid, '(RFC822)')
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
    M.close()
    M.logout()

def cmd_df(args):
    config = load_config()

    print 'Remote\t\t\t\t1K-blocks\tUsed\tAvailable\tStore'
    for store in config.get('rfs','stores').split():
        s=store_c(config,store)
        s.connect()
        tot, used, free = s.df()
        print '%s@%s/%s\t%s\t%s\t%s\t%s' % (s['user'],s['host'],s['folder'],tot/1024,used/1024,free/1024,store)


def cmd_rm(args):
    config = load_config()

    store, uid = args.msgid.split(':')
    M = imap_connect(config.get(store, 'host'),config.get(store, 'user'),config.get(store, 'password'),config.get(store, 'mailbox'))

    #Move to Trash, then flag as deleted, then expunge it
    #M.list()
    #FIXME this is gmail specific
    res = M.uid('copy', uid, '[Gmail]/Trash')
    new_uid=M.untagged_responses['COPYUID'][0].split()[2]

    M.uid('store',uid, '+FLAGS', '\\Deleted')
    M.uid('expunge',uid)
    M.select('[Gmail]/Trash')
    M.uid('store',new_uid, '+FLAGS', '\\Deleted')
    M.uid('expunge',new_uid)
    M.close()
    M.logout()

def cmd_dump(args):
    config = load_config()

    store, uid =args.msgid.split(':')
    M = imap_connect(config.get(store, 'host'),config.get(store, 'user'),config.get(store, 'password'),config.get(store, 'mailbox'))

    typ, data = M.uid('fetch', uid, '(RFC822)')
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
            print part.get_payload(decode=True)
    M.close()
    M.logout()
 
config = load_config()
my_confd_stores = config.get('rfs','stores').split()
my_stores={}
for s in my_confd_stores:
    my_stores[s]=store_c(config, s)

parser = argparse.ArgumentParser(description='RFS, remote file store.')
parser.add_argument('-d',action='store_true',help='-d debug',dest='debug')
subparsers = parser.add_subparsers()

parser_list = subparsers.add_parser('ls', help='ls help')
parser_list.add_argument('-l', action='store_true',help='long format',dest='format_long')
parser_list.add_argument('-msgid', help='List only the specified msgid')
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
parser_dump.add_argument('msgid', help='msgid to dump')
parser_dump.set_defaults(func=cmd_dump)

args = parser.parse_args()
args.func(args)
