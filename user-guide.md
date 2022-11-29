#VAIF User Guide
*(If any problem occurs, please refer to the troubleshooting section at the end of this guide)*

To provide the best user experience, we decided to open-source VAIF with its codebase + a cloudlab profile that helps set up VAIF and the corresponding experiment's environment.

## Prerequisite
- CloudLab](https://www.cloudlab.us/) account
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

## [Optional] Config Pythia (change number of compute node in openstack)

- If one wants more than more compute node in openstack and chages “number of compute nodes” during experiment creation to >1, please follow the following steps
  - If not, skip this section.


- One should SSH into ctl node after experiment creation and auto-setup finished
Check the config /etc/pythia/controller.toml -- it should reflect the actual ctl and cp nodes. If one has 1 compute node, fix config (delete cp-2 and cp-3).

## Set search space for Pythia (Offline Profiling)

- SSH into ctl node
- Sudo su geniuser
  - Geniuser is a dummy user created by CloudLab itself, which we made use of it to get rid of orgnazition dependencies.
- Cd to geniuser’s directory and run `~/pythia/workloads/offline_profiling.sh`
  - It takes around an hour to run
- Go to pythia directory and run `pythia manifest ~/offline_traces.txt`

## Start and Stop Continuous Loop:
- Go to pythia directory and run pythia’s continuous loop
`sudo RUST_BACKTRACE=1 cargo run --bin pythia_controller pythia_out 2>&1 | tee pythia_logs`
- Run continuous workload(an example workload) while pythia continuous loop is up
  - Open a new terminal window and ssh into the controller instance
  - Go to geniuser’s directory and run `~/pythia/workloads/continuous_workload.sh`
- To stop pythia continous loop and continuous workload
  - Pythia: simply exit
  - Continous workload: ` kill $(ps aux | grep workload | awk '{print $2}')`

## Problem Injection:


## Get Results of Analysis:



## Troubleshooting:


