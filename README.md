backrunner
====================
This is a backup proxy running on top of the [RIFT](http://doc.reverbrain.com/rift:rift) proxy.
Its main goal is to write not only multiple data objects into the RIFT bucket, but also to create a backup in the same bucket. Main backup goal is to prevent accidental update or deletion of the main object.

When you delete object via backrunner only primary object is being removed, backup copy is marked as 'to be removed in 2 days'.
Running 'delete' application periodically will actually remove objects from the storage if its life cycle is over (2 days actually passed after object was marked as to be removed).

API
====================
Upload: POST http://108.61.155.67:80/upload/filename where 'filename' is your key
You will receive following json in respose:
```json
{"bucket":"bucket:32.12","primary":{"get":"GET http://108.61.155.67:80/get/bucket:32.12/test.txt","update":"POST http://108.61.155.67:80/upload/bucket:32.12/test.txt","delete":"POST http://storage.coub.com:9090/delete/bucket:32.12/test.txt","key":"test.txt","reply":"{\n    \"info\": [\n        {\n            \"id\": \"29e9a7ed62b79df8728c0ad461fcef54b8818ddba70461b1f0787fa4d85421510b0823917e30ee3c30dcb088771a27b3bb118a3817323dd5ad4007ce04f2884d\",\n            \"csum\": \"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\",\n            \"filename\": \"/mnt/ell.12/elliptics/data/data-0.0\",\n            \"size\": 62,\n            \"offset-within-data-file\": 104859111,\n            \"mtime\": {\n                \"time\": \"2014-05-01 EDT 17:12:03.807160\",\n                \"time-raw\": \"1398978723.807160\"\n            },\n            \"server\": \"108.61.155.67:1037\"\n        },\n        {\n            \"id\": \"29e9a7ed62b79df8728c0ad461fcef54b8818ddba70461b1f0787fa4d85421510b0823917e30ee3c30dcb088771a27b3bb118a3817323dd5ad4007ce04f2884d\",\n            \"csum\": \"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\",\n            \"filename\": \"/mnt/ell.20/elliptics/data/data-0.0\",\n            \"size\": 62,\n            \"offset-within-data-file\": 105012740,\n            \"mtime\": {\n                \"time\": \"2014-05-01 EDT 17:12:03.807160\",\n                \"time-raw\": \"1398978723.807160\"\n            },\n            \"server\": \"108.61.155.69:1045\"\n        }\n    ]\n}\n"},"backup":{"get":"GET http://108.61.155.67:80/get/bucket:32.12/test.txt.backup","update":"","delete":"","key":"test.txt.backup","reply":"{\n    \"info\": [\n        {\n            \"id\": \"d9244fa330b4f98798369205a775bc43fd84a1f757564987be3c0e9e4877880cb20f69ba3ee90e657c76d3b0814702b2de58953a500121340c028443685b3573\",\n            \"csum\": \"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\",\n            \"filename\": \"/mnt/ell.20/elliptics/data/data-0.0\",\n            \"size\": 62,\n            \"offset-within-data-file\": 105012582,\n            \"mtime\": {\n                \"time\": \"2014-05-01 EDT 17:12:03.825930\",\n                \"time-raw\": \"1398978723.825930\"\n            },\n            \"server\": \"108.61.155.69:1045\"\n        },\n        {\n            \"id\": \"d9244fa330b4f98798369205a775bc43fd84a1f757564987be3c0e9e4877880cb20f69ba3ee90e657c76d3b0814702b2de58953a500121340c028443685b3573\",\n            \"csum\": \"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\",\n            \"filename\": \"/mnt/ell.13/elliptics/data/data-0.0\",\n            \"size\": 62,\n            \"offset-within-data-file\": 154683,\n            \"mtime\": {\n                \"time\": \"2014-05-01 EDT 17:12:03.825930\",\n                \"time-raw\": \"1398978723.825930\"\n            },\n            \"server\": \"108.61.155.67:1038\"\n        }\n    ]\n}\n"}}
```

To further update, get or delete your object use appropriate `primary::update`, `primary::get` or `primary::delete` URL.
To read 'backup' copy use `backup::get` URL.

Backup copy is disabled by default, to turn it on you have to run proxy with `-backup` parameter.

Backup copy deletion
====================
```
$ go build -a delete.go buckets.go
$ delete -remote=108.61.155.67:1025:2
```

It will automatically find out all buckets from metadata groups, access their data groups, list objects and remote appropriate objects.
`-help` displays all supported options.

Proxy build
====================
```
$ go build -a proxy.go buckets.go
$ proxy -remote=108.61.155.67:80 -listen=:9090
```

Proxy will run on 9090 port and forward requests to `-addr` host (please note, you should not use 'http://' scheme there, it will be added automatically.
`-help` displays all supported options.

Please note that every new key being uploaded is actually written 4 times into the storage by proxy: 2 primary and 2 backup copies are made. That's why writing a huge objects may take a little while.

Example:
```
$ curl -o /dev/stdout --data-binary @test.100m "http://108.61.155.67:9090/upload/test1.100m"
```

You will receive a json telling you where your file was placed. If you run the same command again it can rewrite your data and your backup copy. If you only want to update primary key, you must use `primary::update` URL from received json.
