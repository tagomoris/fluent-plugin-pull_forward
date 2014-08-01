# fluent-plugin-pull_forward

[Fluentd](http://fluentd.org) input/output plugin to forward data, by pulling/request-based transportation, over HTTPS.

We can do with pull_forward:
* transfer data into hosts in firewall by pulling
* protect transferring route by HTTPS and basic authentication
* fetch Fluentd events as JSON by HTTPS from any processes

![plugin image](https://raw.githubusercontent.com/tagomoris/fluent-plugin-pull_forward/master/misc/plugin_image.png)

## Configuration

### PullForwardOutput

Configure output plugin to transfer fluentd events to another fluentd nodes.

```apache
<match dummy>
  type pull_forward
  
  buffer_path    /home/myname/tmp/fluentd_event.buffer
  flush_interval 1m   ## default 1h
  
  self_hostname      ${hostname}
  cert_auto_generate yes
  # or
  # "cert_file_path PATH", "private_key_path PATH" and "private_key_passphrase ..."
  
  <user>
    username tagomoris
    password foobar
  </user>
  <user>
    username repeatedly
    password booo
  </user>
</match>
```

PullForwardOutput uses PullPoolBuffer plugin. **DO NOT MODIFY buffer_type**. It uses buffer file, so `buffer_path` is required, and Not so short values are welcome for `flush_interval` because PullPoolBuffer make a file per flushes (and these are not removed until fetches of cluent/in\_pull\_forward).

PullForward always requires SSL and basic authentication. SSL options and `<user>` sections are also required.

### PullForwardInput

Configure input plugin to fetch fluentd events from another fluentd nodes.

```apache
<source>
  type pull_forward
  
  fetch_interval 10s
  timeout 10s
  
  <server>
    host host1.on.internet.example.com
    username tagomoris
    password foobar
  </server>
  <server>
    host host2.on.internet.example.com
    username tagomoris
    password foobar
  </server>
</source>
```

PullForwardInput can fetch events from many nodes of `<server>`.

### HTTPS fetch

We can fluentd events from PullForwardOutput by normal HTTPS.

```
$ curl -k -s --user tagomoris:foobar https://localhost:24280/
[
  [ "test.foo", 1406915165, { "pos": 8, "hoge": 1 } ],
  [ "test.foo", 1406915168, { "pos": 9, "hoge": 1 } ],
  [ "test.foo", 1406915173, { "pos": 0, "hoge": 0 } ]
]
```

## TODO

* TESTS!

## Copyright

* Copyright (c) 2014- TAGOMORI Satoshi (tagomoris)
* License
  * Apache License, Version 2.0
