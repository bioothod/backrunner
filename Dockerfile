FROM reverbrain/backrunner

RUN	apt-get update && \
	apt-get upgrade -y

RUN 	. /etc/profile.d/go.sh && \
	cd /root/go/src/github.com/bioothod/elliptics-go/elliptics && \
	git pull && \
	go install && \
	cd /root/go/src/github.com/bioothod/backrunner && \
	git pull && \
	go install; \
    	rm -rf /var/lib/apt/lists/*

EXPOSE 9090 80
