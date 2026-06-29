DEST=$1
scp -r updated_cloudlab_setup/ "$DEST:~/"
ssh -f $DEST "cd ~/updated_cloudlab_setup && chmod +x experiment_setup.sh && ./experiment_setup.sh && nohup python experiment.py > script.log 2>&1 < /dev/null &"