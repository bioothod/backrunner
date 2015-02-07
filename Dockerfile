FROM reverbrain/backrunner

RUN	apt-get update && \
	apt-get upgrade -y

RUN 	. /etc/profile.d/go.sh && \
	cd /root/go/src/github.com/bioothod/elliptics-go/elliptics && \
	git checkout master && \
	git branch -v && \
	git pull && \
	go install && \
	echo "Go binding has been updated" && \
	cd /root/go/src/github.com/bioothod/backrunner && \
	git checkout master && \
	git branch -v && \
	git pull && \
	go install && \
	echo "Backrunner has been updated" ;\
    	rm -rf /var/lib/apt/lists/*

EXPOSE 9090 80
