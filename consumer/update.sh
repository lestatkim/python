systemctl stop consumer.service
git pull
if [ ! -f "config.py" ]; 
    cp config.py.tpl config.py; 
fi
systemctl start consumer.service
