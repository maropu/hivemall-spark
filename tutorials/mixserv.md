hivemall-spark similarly does model mixing in cooperation with hivemall MIX servers.
Basically, HiveContext has the same functionality for model mixing and a detaild tutorial can be found
in [How to use Model Mixing](https://github.com/myui/hivemall/wiki/How-to-use-Model-Mixing).
This tutorial shows how to use helper scripts to start/stop MIX servers and
how to use them in DataFrame in hivemall-spark.

Installation
--------------------
To launch MIX servers with the scripts, you should list the hosts in $HIVEMALL_HOME/conf/servers
where you intend to start MIX servers, one per line.
The scripts access these hosts via ssh and so reqires password-less (using a private key) logins to be setup.

Here, mix01, mix02, and mix03 are assumed to be hosts for MIX servers.
```
// Settings for each host
$ for host in mix01 mix02 mix03; do \
   ssh -o StrictHostKeyChecking=no $host \
     "git clone https://github.com/maropu/hivemall-spark.git; \
       echo -e '\nexport HIVEMALL_HOME=`cd hivemall-spark; pwd`' >> ~/.bashrc"; \
  done

// Lists the hosts for MIX servers
$ echo -e 'mix01\nmix02\nmix03' > $HIVEMALL_HOME/conf/servers
```

Once the setup finished, you can handle MIX servers with the following scripts.
```
$ cd $HIVEMALL_HOME/sbin

// Starts all the MIX servers
$ ./start-servers.sh

// Show running statuses in the MIX servers
$ ./status-servers.sh

// Stop the MIX servers
$ ./stop-servers.sh
```

Finally, the comma-separated host names are put in HIVEMALL_MIX_SERVERS
to cooperate the MIX servers in DataFrame.
```
$ echo -e '\nHIVEMALL_MIX_SERVERS=mix01,mix02,mix03' >> ~/.bashrc
```

