# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2015 MonetDB B.V.
# All Rights Reserved.

"""
This is the python2 implementation of the mapi protocol.
"""

import socket
import logging
import struct
import hashlib
import os
from io import BytesIO
from select import select
from greenlet import greenlet

from monetdb.exceptions import (OperationalError, DatabaseError, ProgrammingError,
                                NotSupportedError, InterfaceError)

logger = logging.getLogger("monetdb")
logger.addHandler(logging.NullHandler())

MAX_PACKAGE_LENGTH = (1024 * 8) - 2

MSG_PROMPT = ""
MSG_MORE = "\1\2\n"
MSG_INFO = "#"
MSG_ERROR = "!"
MSG_Q = "&"
MSG_QTABLE = "&1"
MSG_QUPDATE = "&2"
MSG_QSCHEMA = "&3"
MSG_QTRANS = "&4"
MSG_QPREPARE = "&5"
MSG_QBLOCK = "&6"
MSG_HEADER = "%"
MSG_TUPLE = "["
MSG_TUPLE_NOSLICE = "="
MSG_REDIRECT = "^"
MSG_OK = "=OK"

STATE_INIT = 0
STATE_READY = 1

POLL_READ = 0
POLL_WRITE = 1
POLL_OK = 2


# noinspection PyExceptionInherit
class Connection(object):
    """
    MAPI (low level MonetDB API) connection
    """

    def __init__(self):
        self.state = STATE_INIT
        self._result = None
        self.socket = ""
        self.hostname = ""
        self.port = 0
        self.username = ""
        self.password = ""
        self.database = ""
        self.language = ""
        self.connectionclosed = False

    def connect(self, database, username, password, language, hostname=None,
                port=None, unix_socket=None, async=False):
        """ setup connection to MAPI server

        unix_socket is used if hostname is not defined.
        """

        if hostname and hostname[:1] == '/' and not unix_socket:
            unix_socket = '%s/.s.monetdb.%d' % (hostname, port)
            hostname = None
        if not unix_socket and os.path.exists("/tmp/.s.monetdb.%i" % port):
            unix_socket = "/tmp/.s.monetdb.%i" % port
        elif not hostname:
            hostname = 'localhost'

        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.language = language
        self.unix_socket = unix_socket
        self.async = async

        self.__isexecuting = False

        if hostname:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # For performance, mirror MonetDB/src/common/stream.c socket settings.
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 0)
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.socket.connect((hostname, port))
        else:
            self.socket = socket.socket(socket.AF_UNIX)
            self.socket.connect(unix_socket)
            if self.language != 'control':
                self.socket.send('0'.encode())  # don't know why, but we need to do this

        if not (self.language == 'control' and not self.hostname):
            # control doesn't require authentication over socket
            self._login()

        self.state = STATE_READY

    def _login(self, iteration=0):
        """ Reads challenge from line, generate response and check if
        everything is okay """

        challenge = self._getblock()
        response = self._challenge_response(challenge)
        self._putblock(response)
        prompt = self._getblock().strip()

        if len(prompt) == 0:
            # Empty response, server is happy
            pass
        elif prompt == MSG_OK:
            pass
        elif prompt.startswith(MSG_INFO):
            logger.info("%s" % prompt[1:])

        elif prompt.startswith(MSG_ERROR):
            logger.error(prompt[1:])
            raise DatabaseError(prompt[1:])

        elif prompt.startswith(MSG_REDIRECT):
            # a redirect can contain multiple redirects, for now we only use
            # the first
            redirect = prompt.split()[0][1:].split(':')
            if redirect[1] == "merovingian":
                logger.debug("restarting authentication")
                if iteration <= 10:
                    self._login(iteration=iteration + 1)
                else:
                    raise OperationalError("maximal number of redirects "
                                           "reached (10)")

            elif redirect[1] == "monetdb":
                self.hostname = redirect[2][2:]
                self.port, self.database = redirect[3].split('/')
                self.port = int(self.port)
                logger.info("redirect to monetdb://%s:%s/%s" %
                            (self.hostname, self.port, self.database))
                self.socket.close()
                self.connect(self.hostname, self.port, self.username,
                             self.password, self.database, self.language)

            else:
                raise ProgrammingError("unknown redirect: %s" % prompt)

        else:
            raise ProgrammingError("unknown state: %s" % prompt)

    def disconnect(self):
        """ disconnect from the monetdb server """
        self.state = STATE_INIT
        self.socket.close()

    def cmd(self, operation, f=None):
        """ put a mapi command on the line"""
        logger.debug("executing command %s" % operation)

        if self.state != STATE_READY:
            raise ProgrammingError

        while True:
            self._putblock(operation)
            response = self._getblock()
            if not len(response):
                return ""
            elif response.startswith(MSG_OK):
                return response[3:].strip() or ""
            elif response == MSG_MORE:
                if f is not None:
                    operation = f.read(4096)
                    if operation != "":
                        continue
                return self.cmd("")
            elif response[0] in [MSG_Q, MSG_HEADER, MSG_TUPLE]:
                return response
            elif response[0] == MSG_ERROR:
                raise OperationalError(response[1:])
            elif (self.language == 'control' and not self.hostname):
                if response.startswith("OK"):
                    return response[2:].strip() or ""
                else:
                    return response
            else:
                raise ProgrammingError("unknown state: %s" % response)

    def poll(self):
        if not self.async:
            raise InterfaceError("Poll called on a synchronous connection")
        if not self.isexecuting():
            raise InterfaceError("No command is currently executing")

        state = self._greenlet.switch()
        if self._greenlet.dead:  # task has completed
            self.__isexecuting = False
            return POLL_OK

        return state

    def runasync(self, function):
        self.__isexecuting = True
        self._greenlet = greenlet(function)

    def isexecuting(self):
        return self.__isexecuting

    def fileno(self):
        return self.socket.fileno()

    def _challenge_response(self, challenge):
        """ generate a response to a mapi login challenge """
        challenges = challenge.split(':')
        salt, identity, protocol, hashes, endian = challenges[:5]
        password = self.password

        if protocol == '9':
            algo = challenges[5]
            try:
                h = hashlib.new(algo)
                h.update(password.encode())
                password = h.hexdigest()
            except ValueError as e:
                raise NotSupportedError(e.message)
        else:
            raise NotSupportedError("We only speak protocol v9")

        h = hashes.split(",")
        if "SHA1" in h:
            s = hashlib.sha1()
            s.update(password.encode())
            s.update(salt.encode())
            pwhash = "{SHA1}" + s.hexdigest()
        elif "MD5" in h:
            m = hashlib.md5()
            m.update(password.encode())
            m.update(salt.encode())
            pwhash = "{MD5}" + m.hexdigest()
        else:
            raise NotSupportedError("Unsupported hash algorithms required"
                                    " for login: %s" % hashes)

        return ":".join(["BIG", self.username, pwhash, self.language,
                         self.database]) + ":"

    def _getblock(self):
        """ read one mapi encoded block """
        if (self.language == 'control' and not self.hostname):
            return self._getblock_socket()  # control doesn't do block
                                            # splitting when using a socket
        else:
            return self._getblock_inet()

    def _getblock_inet(self):
        result = BytesIO()
        last = 0
        while not last:
            flag = self._getbytes(2)
            unpacked = struct.unpack('<H', flag)[0]  # little endian short
            length = unpacked >> 1
            last = unpacked & 1
            result.write(self._getbytes(length))
        result_str = result.getvalue()
        return result_str.decode()

    def _getblock_socket(self):
        buffer = BytesIO()
        while True:
            x = self.socket.recv(1)
            if len(x):
                buffer.write(x)
            else:
                break
        return buffer.getvalue().strip()

    def _getbytes(self, bytes_):
        """Read an amount of bytes from the socket"""
        result = BytesIO()
        count = bytes_
        while count > 0:
            if self.async:
                parent = greenlet.getcurrent().parent
                # Switch to parent greenlet if async and no data ready to read
                while parent and not select([self.socket.fileno()], [], [], 0)[0]:
                    parent.switch(POLL_READ)
            recv = self.socket.recv(count)
            if len(recv) == 0:
                self.connectionclosed = True
                raise OperationalError("Server closed connection")
            count -= len(recv)
            result.write(recv)
        return result.getvalue()

    def _putblock(self, block):
        """ wrap the line in mapi format and put it into the socket """
        if (self.language == 'control' and not self.hostname):
            return self.socket.send(block.encode())  # control doesn't do block
                                            # splitting when using a socket
        else:
            self._putblock_inet(block)

    def _putblock_inet(self, block):
        pos = 0
        last = 0
        while not last:
            data = block[pos:pos + MAX_PACKAGE_LENGTH].encode()
            length = len(data)
            if length < MAX_PACKAGE_LENGTH:
                last = 1
            flag = struct.pack('<H', (length << 1) + last)


            if self.async:
                # Switch to parent greenlet if async and socket not ready to accept data
                parent =greenlet.getcurrent().parent
                while parent and not select([], [self.socket.fileno()], [], 0)[1]:
                    parent.switch(POLL_WRITE)

            self.socket.send(flag)
            self.socket.send(data)
            pos += length

    def __del__(self):
        if self.socket:
            self.socket.close()

    def __repr__(self):
        return "<%s.%s object at 0x%x; url: 'monetdb://%s:%s/%s'>" % (
            self.__class__.__module__, self.__class__.__name__, id(self), self.hostname, self.port,
            self.database)

#backwards compatiblity
Server = Connection
