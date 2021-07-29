# batchq

## description
This is a simple distributed job execution program  


## setup
### vscode
```bash
git clone git@github.com:cbh34680/batchq.git
cd batchq/
python -m venv vsc.venv
. ./vsc.venv/bin/activate
pip install -U pip
pip install -r requirements.txt
python ./work.d/sys/make-config-py.txt
sh linux-make-dotenv.sh
code ./vsc.code-workspace
```

### linux
```bash
mkdir -p ~/virtualenv
python -m venv ~/virtualenv/py38
. ~/virtualenv/py38/bin/activate
git clone git@github.com:cbh34680/batchq.git
cd batchq/
pip install -U pip
pip install -r requirements.txt
cp easy-clean.sh clean.sh
cp easy-run.sh run.sh
cp easy-stop.sh stop.sh
sed -i 's/10\.96\.155\.95/127.0.0.1/' clean.sh
sh clean.sh
sh run.sh
```
