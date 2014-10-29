### Steps to run in production for Mac/Linux

* Run zookepper on nimbus machine. 

	```
	$ cd /usr/share/zookeeper/bin
	$ sudo ./zkServer.sh start

	Output:
	Using config: /etc/zookeeper/conf/zoo.cfg
	Starting zookeeper ... STARTED
	```

* Check zookeeper status:

    ```
    $ sudo ./zkServer.sh status
    ```           

* Start the nimbus daemon on master machine. 

	```
      $ storm nimbus
	```


* Run `jps` command to check nimbus start.

	```
      $ jps

      xxxx Jps
      yyyy nimbus
	```

* Start the supervisor daemon on worker machine. 

	```
     $ storm supervisor
	```
	
* Run `jps` command to check supervisor start.

    ```
      $ jps

      xxxx Jps
      yyyy supervisor
      zzzz nimbus
    ```      

* Run storm ui

    ```
      $ storm ui
    ```

* Point your web browser to http://{nimbus host}:8080
