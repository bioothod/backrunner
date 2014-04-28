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
{"bucket":"bucket:22.31","primary":{"get":"GET http://108.61.155.67:80/get/qwerty?bucket=bucket:22.31","update":"POST http://108.61.155.67:80/upload/qwerty?bucket=bucket:22.31","delete":"POST http://localhost:9090/delete/qwerty?bucket=bucket:22.31","key":"qwerty","reply":"{\n    \"info\": [\n        {\n            \"id\": \"ba88577c7c25d13f8d6a6eae7416143345d8f3aa5109e14492be40eb3b662e9805d244e2270b5d771a7721f7f009c6006cdcd299a24686c03df19ccfff2b7a7a\",\n            \"csum\": \"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\",\n            \"filename\": \"/mnt/ell.15/elliptics/data/data-0.0\",\n            \"size\": 67,\n            \"offset-within-data-file\": 144,\n            \"mtime\": {\n                \"time\": \"2014-04-27 EDT 19:25:43.204605\",\n                \"time-raw\": \"1398641143.204605\"\n            },\n            \"server\": \"108.61.155.68:1040\"\n        },\n        {\n            \"id\": \"ba88577c7c25d13f8d6a6eae7416143345d8f3aa5109e14492be40eb3b662e9805d244e2270b5d771a7721f7f009c6006cdcd299a24686c03df19ccfff2b7a7a\",\n            \"csum\": \"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\",\n            \"filename\": \"/mnt/ell.7/elliptics/data/data-0.0\",\n            \"size\": 67,\n            \"offset-within-data-file\": 144,\n            \"mtime\": {\n                \"time\": \"2014-04-27 EDT 19:25:43.204605\",\n                \"time-raw\": \"1398641143.204605\"\n            },\n            \"server\": \"108.61.155.69:1032\"\n        }\n    ]\n}\n"},"backup":{"get":"GET http://108.61.155.67:80/get/qwerty.backup?bucket=bucket:22.31","update":"","delete":"","key":"qwerty.backup","reply":"{\n    \"info\": [\n        {\n            \"id\": \"cabea6d88e588f9d1c7c8bf9deee666081d86f01fcf624fff83f41da766065be5b608ca008bdf8f578306ef28cd3f6a660fc5cacc01b6e964eb8c02a9b14c24b\",\n            \"csum\": \"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\",\n            \"filename\": \"/mnt/ell.20/elliptics/data/data-0.0\",\n            \"size\": 67,\n            \"offset-within-data-file\": 392,\n            \"mtime\": {\n                \"time\": \"2014-04-27 EDT 19:25:43.352626\",\n                \"time-raw\": \"1398641143.352626\"\n            },\n            \"server\": \"108.61.155.68:1045\"\n        },\n        {\n            \"id\": \"cabea6d88e588f9d1c7c8bf9deee666081d86f01fcf624fff83f41da766065be5b608ca008bdf8f578306ef28cd3f6a660fc5cacc01b6e964eb8c02a9b14c24b\",\n            \"csum\": \"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\",\n            \"filename\": \"/mnt/ell.9/elliptics/data/data-0.0\",\n            \"size\": 67,\n            \"offset-within-data-file\": 144,\n            \"mtime\": {\n                \"time\": \"2014-04-27 EDT 19:25:43.352626\",\n                \"time-raw\": \"1398641143.352626\"\n            },\n            \"server\": \"108.61.155.69:1034\"\n        }\n    ]\n}\n"}}
```

To further update, get or delete your object use appropriate `primary::update`, `primary::get` or `primary::delete` URL.
To read 'backup' copy use `backup::get` URL.


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
