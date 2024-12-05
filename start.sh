#!/bin/bash
__dirname=$(cd $(dirname "$0"); pwd -P)
cd ${__dirname}

echo -e "\033[92m"      
echo " _       __     __    ____  ____  __  ___"
echo "| |     / /__  / /_  / __ \/ __ \/  |/  /"
echo "| | /| / / _ \/ __ \/ / / / / / / /|_/ / "
echo "| |/ |/ /  __/ /_/ / /_/ / /_/ / /  / /  "
echo "|__/|__/\___/_.___/\____/_____/_/  /_/   "
echo                          
echo -e "\033[39m"

# Função para exibir mensagem de quase pronto
almost_there(){
    echo 
    echo "===================="
    echo "You're almost there!"
    echo "===================="
}

# Verificação da versão do Python
python -c "import sys;ret = 1 if sys.version_info <= (3, 0) else 0;print('Checking python version... ' + ('3.x, good!' if ret == 0 else '2.x'));sys.exit(ret);"
if [ $? -ne 0 ]; then
	almost_there
    echo -e "\033[33mYour system is currently using Python 2.x. You need to install or configure your system to use Python 3.x. Check out http://docs.python-guide.org/en/latest/dev/virtualenvs/ for information on how to setup Python 3.x alongside your Python 2.x install.\033[39m"
    echo
    exit
fi

# Configuração para depuração remota
if [ "$ENABLE_DEBUGPY" = "True" ]; then
    echo "Installing debugpy for remote debugging..."
    pip install debugpy
fi

# Configuração do ambiente de desenvolvimento
if [ "$1" = "--setup-devenv" ] || [ "$2" = "--setup-devenv" ]; then
    echo "Setting up development environment..."

    git submodule update --init
    
    echo "Installing npm dependencies..."
    npm install

    cd nodeodm/external/NodeODM
    npm install

    cd /webodm

    echo "Installing pip requirements..."
    pip install -r requirements.txt

    echo "Building translations..."
    python manage.py translate build --safe

    echo "Starting webpack in watch mode..."
    webpack --watch &
fi

echo "Running migrations..."
python manage.py migrate

# Adicionar nós padrão, se configurado
if [[ "$WO_DEFAULT_NODES" > 0 ]]; then
    i=0
    while [ $i -ne "$WO_DEFAULT_NODES" ]
    do
        i=$(($i+1))
        NODE_HOST=$(python manage.py getnodehostname webodm_node-odm_$i)
        python manage.py addnode $NODE_HOST 3000 --label node-odm-$i
    done
fi

# Executar servidor Django no modo desenvolvimento
if [ "$1" = "--setup-devenv" ] || [ "$2" = "--setup-devenv" ]; then
    if [ "$ENABLE_DEBUGPY" = "True" ]; then
        echo "Starting Django with debugpy on port 5678..."
        python -m debugpy --listen 0.0.0.0:5678 --wait-for-client manage.py runserver 0.0.0.0:8000
    else
        echo "Starting Django development server..."
        python manage.py runserver 0.0.0.0:8000
    fi
    exit
fi

# Caso contrário, inicie normalmente com nginx e gunicorn
proto="http"
if [ "$WO_SSL" = "YES" ]; then
    proto="https"
fi

congrats(){
    (sleep 5; echo
    echo "Trying to establish communication..."
    status=$(curl --max-time 300 -L -s -o /dev/null -w "%{http_code}" "$proto://localhost:8000")

    if [[ "$status" = "200" ]]; then
        echo -e "\033[92m"      
        echo "Congratulations! └@(･◡･)@┐"
        echo ==========================
        echo -e "\033[39m"
        echo "If there are no errors, WebODM should be up and running!"
    else    
        echo -e "\033[93m"
        echo "Something doesn't look right! ¯\_(ツ)_/¯"
        echo "The server returned a status code of $status when we tried to reach it."
        echo ==========================
        echo -e "\033[39m"
        echo "Check if WebODM is running, maybe we tried to reach it too soon."
    fi

    echo -e "\033[93m"
    echo Open a web browser and navigate to $proto://$WO_HOST:$WO_PORT
    echo -e "\033[39m") &
}

# Execução final com nginx e gunicorn
congrats
nginx -c $(pwd)/nginx/nginx.conf
gunicorn webodm.wsgi --bind unix:/tmp/gunicorn.sock --timeout 300000 --max-requests 500 --workers $((2*$(grep -c '^processor' /proc/cpuinfo)+1)) --preload
