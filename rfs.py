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

def hashfile(f):
    hasher=hashlib.sha512()
    with open(f, 'rb') as afile:
        buf = afile.read()
        hasher.update(buf)
    return hasher.hexdigest()

class finfo(dict):
    def __init__(self,fname=None,json_str=None):
        if fname:
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
            self['sha512']=hashfile(fname)
        elif json_str:
            #print json_str
            f=json.loads(json_str)
            #Lift in known keys
            for k in ['fname','atime','ctime','mtime','size','user','group','mode']:
                if f[k]:
                    self[k]=f[k]
                else:
                    self[k]=''

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
        return self['size']

    def get_atime(self):
        return time.strftime('%x %X',time.localtime(self['atime']))
        
    def get_ctime(self):
        return time.strftime('%x %X',time.localtime(self['ctime']))

    def get_mtime(self):
        return time.strftime('%x %X',time.localtime(self['mtime']))

    def get_fname(self):
        return self['fname']

def load_config():
    config = ConfigParser.ConfigParser()
    config.read([ os.path.expanduser('~/.rfs.conf'), 'rfs.conf'])
    if len(config.sections())==0:
        print 'config has no sections.'
        sys.exit(1)
    return config

def imap_connect(host,user,password,mailbox):
    M = imaplib.IMAP4_SSL(host)
    if args.debug:
        M.debug=5
    M.login(user,password)
    typ, num = M.select(mailbox)
    #print "select returned %s" %typ
    if typ != 'OK':
        print 'Folder %s not found, creating it' % mailbox
        M.create(mailbox)
        typ, num = M.select(mailbox)
        if typ != 'OK':
            print 'Failed to create folder. Exiting.'
            sys.exit(1)
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
            print wanted_uids
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
                t_str=t_str+' %s' % finf.get_group()
                t_str=t_str+'\t%s' % finf.get_size()
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

def cmd_put(args):
    config = load_config()

    #Check if a store has been given
    if args.store:
       store=args.store
    else:
        #FIXME: This should be dynamic based on free space etc
        store = config.get('rfs','stores').split()[1]

    msg = email.mime.multipart.MIMEMultipart()
    msg['Subject']='%s' % args.fname

    #Prepare metadata
    info=finfo(fname=args.fname)
    part=email.mime.application.MIMEApplication(info.to_json(),_encoder=email.encoders.encode_base64)
    msg.attach(part)

    f=open(args.fname, 'rb')
    part=email.mime.application.MIMEApplication(f.read())
    #part['Content-Disposition'] = 'attachment'
    msg.attach(part)

    #Start network interactions.
    M = imap_connect(config.get(store, 'host'),config.get(store, 'user'),config.get(store, 'password'),config.get(store, 'mailbox'))
    M.append(config.get(store, 'mailbox'), '', '', msg.as_string())
    M.close()
    M.logout()

def cmd_get(args):
    config = load_config()

    if not ':' in args.msgid:
        print 'msgid has to have <store>:<uid> format'
        sys.exit(1)

    store=args.msgid.split(':')[0]
    uid=args.msgid.split(':')[1]

    M = imap_connect(config.get(store, 'host'),config.get(store, 'user'),config.get(store, 'password'),config.get(store, 'mailbox'))
    
    typ, data = M.uid('fetch', uid, '(RFC822)')
    msg=email.message_from_string(data[0][1])
    if not msg.is_multipart():
        print 'Error, message uid %s in store %s is not multipart' %(uid,store)
        quit()
    #loop the payload to find the relevant parts
    for payload in msg.get_payload():
        try:
            #meta=json.loads(payload.get_payload(decode=True))
            finf = finfo(json_str=payload.get_payload(decode=True))
        except ValueError:
            #Save the ref
            pl=payload
            #Not valid json, continue the loop
            continue
    #FIXME update the metadata as well
    f = open(finf.get_fname(),'wb')
    f.write(pl.get_payload(decode=True))
    f.close()
    M.close()
    M.logout()

def cmd_df(args):
    config = load_config()

    print 'Remote\t\t\t\t1K-blocks\tUsed\tAvailable\tStore'
    for store in config.get('rfs','stores').split():
        M = imap_connect(config.get(store, 'host'),config.get(store, 'user'),config.get(store, 'password'),config.get(store, 'mailbox'))
        q_str=M.getquotaroot('INBOX')
        q_used=q_str[1][1][0].split()[2]
        q_total=q_str[1][1][0].split()[3].split(')')[0]
        q_avail=int(q_total)-int(q_used)
        print '%s@%s/%s\t%s\t%s\t%s\t%s' %(config.get(store, 'user'),config.get(store, 'host'),config.get(store, 'mailbox'),q_total,q_used,q_avail,store)

def cmd_rm(args):
    config = load_config()

    store=args.msgid.split(':')[0]
    uid=args.msgid.split(':')[1]
    M = imap_connect(config.get(store, 'host'),config.get(store, 'user'),config.get(store, 'password'),config.get(store, 'mailbox'))

    #Move to Trash, then flag as deleted, then expunge it
    M.list()
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

    store=args.uid.split(':')[0]
    uid=args.uid.split(':')[1]
    M = imap_connect(config.get(store, 'host'),config.get(store, 'user'),config.get(store, 'password'),config.get(store, 'mailbox'))

    typ, data = M.uid('fetch', uid, '(RFC822)')
    print data[0][1]
    M.close()
    M.logout()
 
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
parser_dump.add_argument('--rfc822', action='store_true', help='dump rfc822 container',dest='rfc822')
parser_dump.add_argument('msgid', help='msgid to dump')
parser_dump.set_defaults(func=cmd_dump)

args = parser.parse_args()
args.func(args)
