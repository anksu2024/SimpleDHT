SimpleDHT
=========

Application to depict the partial working of the Distributed Hast Tables in an environment of 5 AVDs

PROJECT SPECIFICATION: https://docs.google.com/document/d/1L84mUB66fKHNbZyzyYaDjqLA9d96k4_Z1HFpAgcSqhI/pub

TEST SCRIPTS: The test scripts (Depending on the Operating System) can be downloaded from the links provided in the project specifications.

NOTE: FYR the project specifications (Resources) and Test Scripts are also provided in the Respective Folders

8 STEPS TO REALIZE THIS PROJECT:

1) Set present working directory as "Test Scripts" directory.

2) This directory contains the python scripts that are to be executed to test the project

3) Under this directory in the Terminal execute the script 'create_avd.py' to create 5 AVDs Use the command "python create_avd.py 5"

4) Once the AVDs are created, excute the script 'run_avd.py' script to start 5 AVDs For the purpose use the command: "python run_avd.py 5"

5) When the 5 AVDs are up and running, each AVD needs to be assigned a port number

6) To do this execute the script 'set_redir.py' Command to be used: "python set_redir.py 10000" This command will make sure that all the AVDs are connected to each other by a single Server port number 10000

If all the AVDs don't get assigned the respective port number, use "adb kill-server" and after 2 minutes use "adb start-server" on the Terminal

7) Now the Tester is good to go ahead with the execution of the App on the AVDs

8) To start the tester, type the following without qoutes:

"./simpledht-grading.osx path_of_simpledhtapkfile/SimpleDht.apk"
<I have used Mac OS to execute the tester>

Please send in comments or report a bug: sarrafan[at]buffalo[dot]edu