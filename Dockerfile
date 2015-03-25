FROM reverbrain/backrunner

RUN	apt-get update && \
	apt-get upgrade -y

RUN 	. /etc/profile.d/go.sh && \
	VERSION=go1.4.2 && \
	curl -f -I https://storage.googleapis.com/golang/$VERSION.linux-amd64.tar.gz && \
	test `go version | awk {'print $3'}` = $VERSION || \
	echo "Downloading" && \
	curl -O https://storage.googleapis.com/golang/$VERSION.linux-amd64.tar.gz && \
	rm -rf /usr/local/go && \
	tar -C /usr/local -xf $VERSION.linux-amd64.tar.gz && \
	rm -f $VERSION.linux-amd64.tar.gz

RUN 	. /etc/profile.d/go.sh && \
	cd /root/go/src/github.com/bioothod/elliptics-go/elliptics && \
	git checkout master && \
	git pull && \
	git branch -v && \
	go install && \
	echo "Go binding has been updated" && \
	cd /root/go/src/github.com/bioothod/backrunner && \
	git checkout master && \
	git pull && \
	git branch -v && \
	go install && \
	echo "Backrunner has been updated" ;\
    	rm -rf /var/lib/apt/lists/*

EXPOSE 9090 80
