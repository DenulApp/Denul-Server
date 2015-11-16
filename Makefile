OPENSSL=/usr/bin/openssl

setup : server.crt 

my.crt :
	$(OPENSSL) req -x509 -nodes -days 365 -newkey rsa:4096 -keyout server.key -out server.crt
