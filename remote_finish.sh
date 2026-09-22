#start from the tardis-migration-kubernetes repo folder with your node-0 remote name and this will transfer logs and results from a completed experiment
#usage if on linux:
#chmod +x remote_finish.sh
#./remote_finish.sh user@host
DEST=$1
scp "$DEST:~/updated_cloudlab_setup/script.log" .
scp "$DEST:~/updated_cloudlab_setup/exp_results.txt" .
scp -r "$DEST:~/updated_cloudlab_setup/experiment_results/" .