""" Proxy for Parrot devices.

    Finds the first Jumping Sumo it can, re-advertises a proxied version on
    all interfaces.
"""
import collections
import json
import netifaces
import socket
import time
import threading
import zeroconf
import SocketServer


def ip_addresses():
    """ Return all my IP addresses.
    """
    addresses = []
    for interface in netifaces.interfaces():
        try:
            for link in netifaces.ifaddresses(interface)[netifaces.AF_INET]:
                addresses.append(link['addr'])
        except KeyError:
            pass
    return sorted(addresses)


class SumoProxy(object):
    """ Proxy for Jumping Sumo to display data.
    """
    RECV_MAX = 102400

    def __init__(self, repeaters=None):
        """ 'repeaters' argument is list of (ip, port) tuples.

            Each is sent a copy of the data in each direction.
        """
        self._repeaters = [] if repeaters is None else repeaters
        self._zc = zeroconf.Zeroconf()

        # Monkey-patch the UDP socket server to recieve video packets.
        SocketServer.UDPServer.max_packet_size = 65000

    def get_first_sumo(self, service_type='_arsdk-0902._udp.local.'):
        """ Return the IP and INIT port for the first Jumping Sumo you find.
        """
        connection_info = []

        class Listener(object):
            """ A simple listener for the sumo init service.
            """
            def remove_service(self, zc, type_, name):
                """ We're not concerned with the remove_service event.
                """
                pass

            def add_service(self, zc, type_, name):
                """ If we've found the JumpingSumo service, get the info.
                """
                info = zc.get_service_info(type_, name)
                connection_info.extend(
                    (socket.inet_ntoa(info.address), info.port)
                )

        browser = zeroconf.ServiceBrowser(
            self._zc, service_type, Listener()
        )

        wait_time = 30  # Seconds
        started = time.time()
        while len(connection_info) < 2:
            if not browser.is_alive():
                raise Exception('Zeroconf Browser crashed')
            if time.time() - started > wait_time:
                raise Exception(
                    'No Sumo found within {} seconds'.format(wait_time)
                )
            time.sleep(0.1)
        browser.cancel()

        return connection_info[:2]

    def announce_proxy_sumo(self, init_port,
                            service_name='Sumo',
                            service_type='_arsdk-0902._udp.local.'):
        """ Announce the proxied Jumping Sumo on all interfaces.
        """
        for address in ip_addresses():
            iface_service_name = '{}-{}'.format(
                service_name,
                address.replace('.', '-')
            )
            info = zeroconf.ServiceInfo(
                service_type,
                '.'.join((iface_service_name, service_type)),
                socket.inet_aton(address),
                init_port,
                properties={},
            )
            self._zc.register_service(info)

    def proxy_init(self, sumo_ip, init_port):
        """ Proxy the init.
        """
        init_server = None
        return_data = []

        class InitHandler(SocketServer.BaseRequestHandler):
            """ SocketServer handler for init handshake.
            """
            def handle(self):

                # Capture the client's IP address.
                client_ip = self.client_address[0]

                # Get the init request, strip the trailing '\x00', convert to
                # JSON
                data = self.request.recv(SumoProxy.RECV_MAX)
                json_data = json.loads(data[:-1])

                # Grab the d2c port that the client is listening on - this is
                # where it expects to recieve packets. Will probably be 54321.
                client_d2c_port = json_data['d2c_port']

                # Create a new d2c port that the proxy will listen on - this
                # is how we intercept the packets. Will probably be 54322.
                prox_d2c_port = client_d2c_port + 1

                # Modify the init to tell the Sumo to send packets to the
                # proxy's d2c port. We'll pass these on to the client's d2c
                # port.
                json_data['d2c_port'] = prox_d2c_port
                data = json.dumps(json_data) + '\x00'

                # Send on the init.
                sumo_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sumo_sock.connect((sumo_ip, init_port))
                sumo_sock.sendall(data)

                # Get the init response, strip the trailing '\x00', convert to
                # JSON
                data = sumo_sock.recv(SumoProxy.RECV_MAX)
                json_data = json.loads(data[:-1])
                sumo_sock.close()

                # Grab the c2d port that the sumo is listening on - we'll send
                # packets to this later. Will probably be 54321.
                sumo_c2d_port = json_data['c2d_port']

                # Create a new c2d port for the proxy - this is where the
                # client will send packets to and we'll pass them on to the
                # Sumo's c2d port. Will probably be 54320.
                prox_c2d_port = sumo_c2d_port - 1

                # Modify the init response to tell the client to send packets
                # to the proxy's c2d port, where the proxy can pass them on to
                # the Sumo's c2d port.
                json_data['c2d_port'] = prox_c2d_port
                data = json.dumps(json_data) + '\x00'

                # Return the modified init response back to the client.
                self.request.sendall(data)

                return_data.extend((
                    client_ip, (
                        sumo_c2d_port, client_d2c_port,
                        prox_c2d_port, prox_d2c_port,
                    )
                ))

                def tidy():
                    """ Clean up the server.
                    """
                    init_server.shutdown()
                    init_server.server_close()
                threading.Thread(target=tidy).start()

        init_server = SocketServer.TCPServer(('', init_port), InitHandler)
        server_thread = threading.Thread(target=init_server.serve_forever)
        server_thread.start()

        wait_time = 30
        server_thread.join(wait_time)

        # If thread's still alive we didn't have an init
        if server_thread.is_alive():
            raise Exception(
                'No init within {} seconds of announce'.format(wait_time)
            )

        return return_data

    def proxy_session(self, client_ip, sumo_ip, sumo_c2d_port,
                      client_d2c_port, prox_c2d_port, prox_d2c_port):
        """ Proxy a UDP session between client and sumo.
        """

        data_queue = collections.deque([True], maxlen=1)
        send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        repeaters = self._repeaters

        class C2DHandler(SocketServer.BaseRequestHandler):
            """ Handle client to sumo comms.
            """
            def handle(self):
                data_queue.append(True)
                data = self.request[0]
                send_socket.sendto(data, (sumo_ip, sumo_c2d_port))

                # Tee-off the data to another hosts
                for target in repeaters:
                    send_socket.sendto('>'+data, target)


        class D2CHandler(SocketServer.BaseRequestHandler):
            """ Handle sumo to client comms.
            """
            def handle(self):
                data_queue.append(True)
                data = self.request[0]
                send_socket.sendto(data, (client_ip, client_d2c_port))

                # Tee-off the data to another hosts
                for target in repeaters:
                    send_socket.sendto('<'+data, target)

        c2d_server = SocketServer.UDPServer(('', prox_c2d_port), C2DHandler)
        d2c_server = SocketServer.UDPServer(('', prox_d2c_port), D2CHandler)
        t1 = threading.Thread(target=c2d_server.serve_forever)
        t1.daemon = True
        t1.start()
        t2 = threading.Thread(target=d2c_server.serve_forever)
        t2.daemon = True
        t2.start()

        comms_time = 1
        while True:
            try:
                data_queue.pop()
            except IndexError:
                raise Exception(
                    'No comms for more than {} seconds'.format(comms_time)
                )
            time.sleep(comms_time)

    def start(self):
        """ Handle all the things.
        """
        # Find the robot
        print 'Searching for Jumping Sumo...',
        sumo_ip, init_port = self.get_first_sumo()
        print 'Done (found {})!'.format(sumo_ip)

        # Announce equivalent sumo
        print 'Announcing Sumo Proxy...',
        self.announce_proxy_sumo(init_port)
        print 'Done!'

        print 'Waiting for client initiation...',
        client_ip, ports = self.proxy_init(sumo_ip, init_port)
        print 'Done!'

        # If sumo_c2d_port (ports[0]) is zero, Sumo is currently in a session.
        if ports[0] == 0:
            raise Exception(
                'Sumo responded that another client is already connected!'
            )

        print 'Serving session...',
        self.proxy_session(client_ip, sumo_ip, *ports)
        print 'Done!'


def proc_wrapper(repeaters=None):
    """ Run the proxy.
    """
    if repeaters is None:
        repeaters = []
    try:
        proxy = SumoProxy(repeaters)
        proxy.start()
    except Exception as ex:
        print 'Ex: {}'.format(ex)


if __name__ == '__main__':

    import multiprocessing
    print 'Starting...'
    while True:
        proc = multiprocessing.Process(target=proc_wrapper)
        proc.start()
        proc.join()
        proc.terminate()
        print 'Restarting...'
        time.sleep(1)
