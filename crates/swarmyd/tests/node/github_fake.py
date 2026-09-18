"""Local TLS GitHub stand-in; credentials arrive on stdin, never in a file."""
import base64
import http.server
import json
import ssl
import sys

expected = json.loads(sys.stdin.readline())


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        token = expected['second' if self.path.startswith('/second') else 'first']
        basic = 'Basic ' + base64.b64encode(('x-access-token:' + token).encode()).decode()
        authorization = self.headers.get('Authorization', '')
        if not authorization:
            self.send_response(401)
            self.send_header('WWW-Authenticate', 'Basic realm="GitHub"')
            self.end_headers()
            return
        if authorization not in (basic, 'token ' + token, 'Bearer ' + token):
            self.send_response(403)
            self.end_headers()
            return
        git = '/info/refs?' in self.path
        body = b'001e# service=git-upload-pack\n00000000' if git else b'{"ok":true}'
        self.send_response(200)
        self.send_header('Content-Type', 'application/x-git-upload-pack-advertisement' if git else 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        print('authorized', flush=True)


server = http.server.HTTPServer(('127.0.0.1', 0), Handler)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(sys.argv[1], sys.argv[2])
server.socket = context.wrap_socket(server.socket, server_side=True)
print(server.server_port, flush=True)
server.serve_forever()
