#start from the tardis-migration-kubernetes repo folder with your node-0 remote name and this will transfer necessary files and start the experiment
#usage if on linux:
#chmod +x remote_setup.sh
#./remote_setup.sh user@host
DEST=$1
scp -r updated_cloudlab_setup/ "$DEST:~/"
ssh -f $DEST "cd ~/updated_cloudlab_setup && nohup python experiment.py > script.log 2>&1 < /dev/null &"