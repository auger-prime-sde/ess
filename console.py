#!/usr/bin/env python3

"""
 ESS procedure
 remote console server - client
"""

import os
import sys
import traceback
import socket
import _socket
import stat
import select
import logging
import readline
import io
import pickle
import codeop
import inspect
import itertools
from struct import unpack, pack_into

# prompts for client
PS1 = '>c> '
PS2 = '.c. '
CMDQUIT = 'quit'


class Console(object):
    SOCK_NAME = "console"
    TOUT = 3
    INIMSG = """Remote console connected with id:{myid:d}.
Type "quit" to quit\n""" + PS1

    def __init__(self, locs=None, stopme=None, sockname=None):
        """Constructor.
locs - dictionary name: object provided as locals
stopme - callable to return bool (True: stop, False: continue)"""
        self.locs = locs
        self.stopme = stopme if stopme is not None else self._nonstop
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sockname = sockname if sockname is not None else Console.SOCK_NAME
        if os.path.exists(self.sockname):
            os.unlink(self.sockname)
        self.sock.settimeout(self.TOUT)
        self.sock.bind(self.sockname)
        os.chmod(self.sockname, stat.S_IRUSR | stat.S_IWUSR)
        self.sock.listen()
        self.logger = logging.getLogger('Console')
        self.logger.info('listening on %s', self.sockname)
        self.repls = {}
        self.bufsocks = []
        self.sysouts = (sys.stdout, sys.stderr)

    def _nonstop(self):
        return False

    def start(self):
        self.logger.debug('starting REPL')
        while not self.stopme():
            rd_list = [bufsock for bufsock in self.bufsocks
                       if bufsock.state in (BufSocket.ST_READLEN,
                                            BufSocket.ST_READDATA)]
            rd_list.append(self.sock)
            wr_list = [bufsock for bufsock in self.bufsocks
                       if bufsock.state == BufSocket.ST_SENDRESP]
            ex_list = self.bufsocks + [self.sock]
            rd_ready, wr_ready, ex = select.select(
                rd_list, wr_list, ex_list, self.TOUT)
            # new connections
            if self.sock in rd_ready:
                ind = rd_ready.index(self.sock)
                rd_ready.pop(ind)
                conn, addr = self.sock.accept()
                bufsock = BufSocket.copy(conn)
                conn.close()
                bufsock.logger = self.logger
                myid = bufsock.myid
                self.logger.info('new connection id:%d' % myid)
                inimsg = self.INIMSG.format(myid=myid)
                bufsock.prepare_send(BufSocket.wrap(inimsg))
                self.repls[myid] = REPL(myid, self.locs, self.logger)
                self.bufsocks.append(bufsock)
            # process exceptional states
            for bufsock in ex:
                self.logger.warning('socket error id %d', bufsock.myid)
                bufsock.close()
                del self.repls[bufsock.myid]
                self.bufsocks.remove(bufsock)
            # process responses
            for bufsock in wr_ready:
                bufsock.buf_send()
            # process incoming commands
            for bufsock in rd_ready:
                cmd = bufsock.buf_recv()
                if cmd is not None:
                    repl = self.repls[bufsock.myid]
                    resp = repl.process(cmd)
                    bufsock.prepare_send(resp)
            nbufsocks = []
            for bufsock in self.bufsocks:
                if self.repls[bufsock.myid].stop and \
                   bufsock.state == BufSocket.ST_READLEN:
                    bufsock.close()
                if bufsock.state == BufSocket.ST_STOP:
                    del self.repls[bufsock.myid]
                else:
                    nbufsocks.append(bufsock)
            self.bufsocks = nbufsocks

        self.logger.debug('REPL finished')

    def stop(self):
        for bufsock in self.bufsocks:
            bufsock.close()
            del self.repls[bufsock.myid]
        self.bufsocks = []
        if self.sock is not None:
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
            self.sock = None
        if os.path.exists(self.sockname):
            os.unlink(self.sockname)

    def __del__(self):
        self.stop()
        sys.stdout, sys.stderr = self.sysouts


class BufSocket(socket.socket):
    ST_READLEN, ST_READDATA, ST_READDONE, ST_SENDRESP, ST_STOP = range(5)
    id_generator = itertools.count()

    def __init__(self, *args, **kwargs):
        self.logger = kwargs.pop('logger', None)
        super(BufSocket, self).__init__(*args, **kwargs)
        self.myid = next(self.id_generator)
        self.buf = bytearray(4)
        self.view = memoryview(self.buf)
        self.state = BufSocket.ST_READDONE

    @classmethod
    def copy(cls, sock):
        fd = _socket.dup(sock.fileno())
        copy = cls(sock.family, sock.type, sock.proto, fileno=fd)
        copy.settimeout(sock.gettimeout())
        return copy

    def _st_readlen(self):
        """Init BufSocket to state read length"""
        self.state = BufSocket.ST_READLEN
        self.index = 0
        self.remain = 4
        self.length = 4

    def _st_readdata(self, length):
        """Init BufSocket to state read data"""
        self.state = BufSocket.ST_READDATA
        if length > len(self.buf):
            self.buf = bytearray(length)
            self.view = memoryview(self.buf)
        self.index = 0
        self.remain = length
        self.length = length

    def prepare_send(self, msg):
        """Init BufSocket to send response"""
        assert self.state == BufSocket.ST_READDONE
        self.state = BufSocket.ST_SENDRESP
        nlen = len(msg) + 4
        if nlen > len(self.buf):
            self.buf = bytearray(nlen)
            self.view = memoryview(self.buf)
        pack_into('>L', self.buf, 0, len(msg))
        self.view[4:nlen] = msg
        self.index = 0
        self.remain = nlen
        self.length = nlen

    def buf_recv(self):
        """Receive some data from socket into buffer, manage state transition
return complete message or None"""
        assert self.state in (BufSocket.ST_READLEN, BufSocket.ST_READDATA)
        try:
            rlen = self.recv_into(self.view[self.index:], self.remain)
        except (ConnectionResetError, BrokenPipeError):
            rlen = 0
        if rlen == 0:
            if self.logger is not None:
                self.logger.info('id%d: recv error, closing BufSocket',
                                 self.myid)
            self.close()
            return None
        self.remain -= rlen
        if self.remain > 0:
            self.index += rlen
            return None
        if self.state == BufSocket.ST_READLEN:
            length = unpack('>L', self.buf[:4])[0]
            self._st_readdata(length)
        else:
            self.state = BufSocket.ST_READDONE
            # if self.logger is not None:
            #     self.logger.debug('id%d: received %dB message',
            #                       self.myid, self.length)
            return self.view[:self.length]

    def buf_send(self):
        """Send some of scheduled data to socket, manage state transition"""
        assert self.state == BufSocket.ST_SENDRESP
        try:
            slen = self.send(self.view[self.index:self.length])
        except (ConnectionResetError, BrokenPipeError):
            slen = 0
        if slen == 0:
            if self.logger is not None:
                self.logger.debug('id%d: send error, closing BufSocket',
                                  self.myid)
            self.close()
            return
        self.remain -= slen
        if self.remain > 0:
            self.index += slen
        else:
            # if self.logger is not None:
            #     self.logger.debug('id%d: message sent (%d B)',
            #                       self.myid, self.length)
            self._st_readlen()

    def close(self):
        """Shutdown and close underlaying socket, switch to ST_STOP"""
        try:  # for client, the socket may be already destroyed
            self.shutdown(socket.SHUT_RDWR)
            super(BufSocket, self).close()
        except OSError:
            pass
        self.state = BufSocket.ST_STOP

    def __del__(self):
        if self.state != BufSocket.ST_STOP:
            self.close()

    @staticmethod
    def unwrap(payload):
        return pickle.loads(payload)

    @staticmethod
    def wrap(msg):
        return pickle.dumps(msg)


class REPL(object):
    """Read-eval-pring loop implementation"""
    def __init__(self, myid, locs=None, logger=None):
        """Constructor.
myid - BufSocket.myid
globs - dictionary with globals
locs - dictionary with locals
logger - optional logger"""
        self.myid, self.logger = myid, logger
        globs = inspect.currentframe().f_globals
        self.globs = {'__builtins__': globs['__builtins__']}
        self.locs = {}
        if locs is not None:
            self.locs.update(locs)
        self.cmdtxt = ''
        self.stop = False

    def process(self, cmd):
        """Append cmd to the current cmdtxt and try to compile it and exec
cmd - wrapped Python code
resp - wrapped response from python code
"""
        self.cmdtxt += BufSocket.unwrap(cmd)
        if self.cmdtxt.startswith(CMDQUIT):
            self.stop = True
            self.cmdtxt = ''
            if self.logger is not None:
                self.logger.info('id:%d client quit', self.myid)
            return BufSocket.wrap('Bye.')

        sysfps = sys.stdout, sys.stderr
        sys.stderr = sys.stdout = fio = io.StringIO()
        try:
            compile_ok = False
            code = codeop.compile_command(self.cmdtxt)
            if code:
                compile_ok = True
                self.cmdtxt = ''
                exec(code, self.globs, self.locs)
                retval = fio.getvalue() + PS1
            else:
                retval = PS2
        except Exception:  # May be syntax err. or raised exception
            msg = ('Runtime' if compile_ok else 'Compile') + ' exception.\n'
            self.cmdtxt = ''
            traceback.print_exc(file=fio)
            retval = msg + fio.getvalue() + PS1
        sys.stdout, sys.stderr = sysfps
        fio.close()
        return BufSocket.wrap(retval)


def console_client(sockname=None, logger=None):
    if sockname is None:
        sockname = Console.SOCK_NAME
    bufsock = BufSocket(socket.AF_UNIX, socket.SOCK_STREAM, logger=logger)
    bufsock._st_readlen()
    bufsock.connect(sockname)
    stop = False
    while bufsock.state != BufSocket.ST_STOP:
        msg = bufsock.buf_recv()
        if msg is None:
            continue
        elif bufsock.state == BufSocket.ST_STOP or stop:
            print(BufSocket.unwrap(msg))
            break
        cmd = input(BufSocket.unwrap(msg)) + '\n'
        if cmd.startswith(CMDQUIT):
            stop = True
        bufsock.prepare_send(BufSocket.wrap(cmd))
        while bufsock.state == BufSocket.ST_SENDRESP:
            bufsock.buf_send()
    bufsock.close()


if __name__ == '__main__':
    sockname = sys.argv[1] if len(sys.argv) == 2 else None
    console_client(sockname)
