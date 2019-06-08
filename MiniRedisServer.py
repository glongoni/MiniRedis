import SocketServer
import SimpleHTTPServer
from MiniRedis import Redis

PORT = 8080

class RedisHttpHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    def __set_headers(self, status):
        self.send_response(status)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def __retrieveKeyString(self):
        path = self.translate_path(self.path)
        key = path.split('/')[-1]
        return key
        

    def do_GET(self):
        key = self.__retrieveKeyString()
        
        print("Request received: GET " + key)
       
        #Find value
        value = miniRedis.get(key)

        #Return
        self.__set_headers(200)
        self.wfile.write(value)
    
    def do_PUT(self):
        key = self.__retrieveKeyString()
        
        #Retrieves value string
        contentLen = int(self.headers.getheader('content-length', 0))
        value = self.rfile.read(contentLen)

        print("Request received: SET " + key + value)
        
        #Set value
        status = miniRedis.set(key, value)

        #Returns
        self.__set_headers(200)
        self.wfile.write(status)

    def do_DELETE(self):
        key = self.__retrieveKeyString()
        
        print("Request received: DELETE " + key)
       
        #Find value
        deleted = miniRedis.delete(key)

        #Return
        self.__set_headers(200)
        self.wfile.write(deleted)

        
httpd = SocketServer.ThreadingTCPServer(('', PORT),RedisHttpHandler)
miniRedis = Redis.Redis(200)

try:
    print "serving at port", PORT
    httpd.serve_forever()
except KeyboardInterrupt:
	print '^C received, shutting down the web server'
	httpd.socket.close()