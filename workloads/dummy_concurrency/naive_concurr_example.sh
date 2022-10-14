cat naive_concurr_cmds.txt | xargs -I CMD --max-procs=3 bash -c CMD
