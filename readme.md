# Chord Based DHT in Android
A chord based distributed hash table (DHT) built using Android as the distributed systems. All node joins are handled in realtime and position in the rings are alloted by the first avd, avd0. Built as a part of the CSE 586 Distributed Computers course at University at Buffalo.

### Usage
* Create the 2 AVDs using the create_avd.py script: python2.7 create_avd.py 2 /PATH_TO_SDK/
* Run the 2 AVDs using the run_avd.py script: python2.7 run_avd.py 2
* Run the port redirection script using: python2.7 set_redir.py 10000
* Now, run the grading script as follows: 
./simpledht-grading.linux /path/to/sdk

For more information about the grading script, run ./simpledht-grading.linux -h