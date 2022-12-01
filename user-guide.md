# VAIF(Pythia) CloudLab Profile User Guide

*(If any problem occurs, please refer to the troubleshooting section at the end of this guide)*

To provide the best user experience, we decided to open-source VAIF with its codebase + a cloudlab profile that helps set up VAIF and the corresponding experiment's environment.

- [VAIF(Pythia) CloudLab Profile User Guide](#vaifpythia-cloudlab-profile-user-guide)
  - [Prerequisite](#prerequisite)
  - [Experiment creation with the shared profile](#experiment-creation-with-the-shared-profile)
  - [(Optional) Change number of compute node](#optional-change-number-of-compute-node)
  - [Offline Profiling: Set search space for Pythia](#offline-profiling-set-search-space-for-pythia)
  - [Start and Stop pythia](#start-and-stop-pythia)
      - [Start continuous workload](#start-continuous-workload)
      - [Start Pythia (push-the-button)](#start-pythia-push-the-button)
      - [Stop pythia continous loop and continuous workload](#stop-pythia-continous-loop-and-continuous-workload)
  - [Problem Injection](#problem-injection)
      - [Recommended method](#recommended-method)
      - [Alternative problem injection](#alternative-problem-injection)
  - [Get Results of Analysis (a simple case study)](#get-results-of-analysis-a-simple-case-study)
  - [Troubleshooting](#troubleshooting)
      - [Check node stats](#check-node-stats)
      - [check pythia agents \&\& restart pythia agenet if necessary](#check-pythia-agents--restart-pythia-agenet-if-necessary)
      - [When you update pythia server](#when-you-update-pythia-server)
      - [Pythia not compliling](#pythia-not-compliling)


## Prerequisite
- [CloudLab](https://www.cloudlab.us/) account
- Basic knowledge of Linux
- Basic understanding of [VAIF](https://dl.acm.org/doi/abs/10.1145/3472883.3487000 )


## Experiment creation with the shared profile

- Sign in to CloudLab
- Access VAIF’s profile, click “Next”.
- In experiment creation steps
  - Parametrize: simply leave the defaults and click “Next”, 
    - If one wants to create an experiment of more than two machines: change “number of compute nodes” to the desired number. And it needs to update config of Pythia, following the instructions in “config pythia” section. 
    - One may change other parameters, but we cannot guarantee it works.
  - Finalize: name your experiment and select the Utah cluster to run on, and click “Next”
  - Schedule: click “Finish” to create the experiment immediately
     - Your experiment will initialize and then take approximately 1-2 hours for internal setup scripts to run; 
     - One will get an email from the system when the installation phase starts
     - One will get another email when it completes.
     - Then it will be ready to use

## (Optional) Change number of compute node

- If one wants more than more compute node in openstack and chages “number of compute nodes” during experiment creation to >1, please follow the following steps
  - If not, skip this section.


- One should SSH into ctl node after experiment creation and auto-setup finished
Check the config /etc/pythia/controller.toml -- it should reflect the actual ctl and cp nodes. If one has 1 compute node, fix config (delete cp-2 and cp-3).

## Offline Profiling: Set search space for Pythia 

- SSH into ctl node
- Sudo su geniuser
  - Geniuser is a dummy user created by CloudLab itself, which we made use of it to get rid of orgnazition dependencies.
- Cd to geniuser’s directory and run `~/pythia/workloads/offline_profiling.sh`
  - It takes around an hour to run
- Go to pythia directory and run `pythia manifest ~/offline_traces.txt`

## Start and Stop pythia
#### Start continuous workload
- Run continuous workload(an example workload) while pythia continuous loop is up
  - Open a new terminal window and ssh into the controller instance
  - Go to geniuser’s directory and run `~/pythia/workloads/continuous_workload.sh`
#### Start Pythia (push-the-button)
- Go to pythia directory and run pythia’s continuous loop
`sudo RUST_BACKTRACE=1 cargo run --bin pythia_controller pythia_out 2>&1 | tee pythia_logs`
#### Stop pythia continous loop and continuous workload
  - Pythia: simply exit
  - Continous workload: ` kill $(ps aux | grep workload | awk '{print $2}')`

## Problem Injection
#### Recommended method
- Ready problems for NOVA (note that when you apply the patch below, you need to choose one injected problem and change its sleep from 1-> 20 
  - e.g., time.sleep (random.randint(0,20))
- See the git diff file in the drive (problem_injections_nova_diff)
- Apply this to your instance’s nova repo
- Then do `pip_install` under /local/nova
  - `sudo systemctl restart nova-compute.service`
- Create the dummy dir 
  - `sudo mkdir /users/output`
  - `sudo chmod ugo+rwx /users/output/`
- Do above steps for all nodes 

#### Alternative problem injection
- Take a look at the trace (e.g., server_create), and determine the tracepoint to inject latency (e.g., /local/nova/nova/virt/libvirt/imagebackend.py:355)
- Inject with 
  ```
  import random
  import time
  time.sleep(random.randint(0,20))
  ```
- Do this for all nodes (i.e., ctl, cp-1 .,..)
- Create the dummy dir in all nodes
  - sudo mkdir /users/output
  - sudo chmod ugo+rwx /users/output/
- In all nodes, run `pip_install` then `sudo systemctl restart nova-compute.service`
  - Alternatively `restart_openstack_ctl` or `restart_openstack_compute` according to the controller or compute instance
- Then execute a workload


## Get Results of Analysis (a simple case study)

**Max_concurrent_builds**: Too low limit on simultaneous server creations throttles performance (Problem 3 from the paper
- change `max_concurrent_builds` option to a low number (e.g., 2)
- To do this so, go to nova.conf file (/etc/nova/nova.conf)
- Then comment-in the option  `max_concurrent_builds` and set it to 2.
- Finally, restart all the services (including nova)
- Pythia will output results

## Troubleshooting
#### Check node stats 
- `curl --data-binary '{"jsonrpc":"2.0","id":"curltext","method":"read_node_stats","params":[]}' -H 'content-type:application/json' http://cp-1:3030`
#### check pythia agents && restart pythia agenet if necessary
- `systemctl --type=service | grep pythia`
- `sudo journalctl -u pythia.service`
- `sudo systemctl restart pythia `
  - Or stop and start

#### When you update pythia server
- `cargo install --path /local/reconstruction/pythia_server`
- Then `sudo systemctl stop pythia`
- `sudo systemctl start pythia`

#### Pythia not compliling
- Do `cargo run --help`, it compiles pythia and runs it with help arg. 
- Consequently binary file of pythia is generated under  target/release. 
- Then simply copy that bin ( target/release/pythia ) to /users/geniuser/.cargo/bin/, then it is fixed..
